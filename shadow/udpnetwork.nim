import stew/endians2, stew/byteutils, tables, strutils, os
import vendor/nim-libp2p/libp2p, vendor/nim-libp2p/libp2p/protocols/pubsub/rpc/messages
import vendor/nim-libp2p/libp2p/muxers/mplex/lpchannel, vendor/nim-libp2p/libp2p/protocols/ping
import chronos
import random # need since rng leads to "Error: internal error: could not find env param for segmentItRandom"
import sequtils, hashes
from times import getTime, toUnix, fromUnix, `-`, initTime, `$`, inMilliseconds, Duration
from nativesockets import getHostname

export ValidationResult
export shuffle

type
  Network = ref object
    switch: Switch
    gossipSub: GossipSub
    udpTransport: DatagramTransport
    custody: byte
    peers: TableRef[TransportAddress, NetworkPeerId]
    reqs: TableRef[int, Future[void]]
  NetworkMessage* = Message
  NetworkAddress* = TransportAddress
  NetworkPeerId* = ref object
    peerId: PeerId
    address: NetworkAddress
    custody: byte

  reqMessage* = ref object
    msgId*: int
    row*: int
    col*: int
    tout*: byte
  respMessage* = ref object
    code*: byte

  ReqHandler* = proc (m: reqMessage): Future[Option[respMessage]] {.async.}

proc hash*(x: NetworkPeerId): Hash =
  var h: Hash = 0
  h = h !& hash(x.peerId)
  result = !$h

proc newRng*() : auto =
  libp2p.newRng()

proc peerAddr2rng*(peerAddr: NetworkAddress, usecase: auto): Rand =
  ## get an RNG for a given peer
  let seed =  hash((peerAddr, usecase))
  initRand(seed)

proc peerId2rng*(peerId: NetworkPeerId, usecase: auto): Rand =
  ## get an RNG for a given peer
  let seed =  hash((peerId.peerId, usecase))
  initRand(seed)

const ReqCodec* = "/nim-libp2p/req/1.0.0"
type
  ReqProto = ref object of LPProtocol
    #rx: Table[(int, int, int), seq[Future[void]]]
    #tx: Table[int, Future[void]

proc new(T: typedesc[ReqProto], reqHandler: auto): T =
  let reqproto = T()

  # assign the new handler
  reqproto.handler = reqHandler
  reqproto.codec = ReqCodec
  return reqproto

proc getCustody*(n: Network, peerId: NetworkPeerId) : int =
  #parseInt(n.switch.peerStore[AgentBook][peerId.peerId])
  peerId.custody.int

proc msgIdProvider(m: Message): Result[MessageId, ValidationResult] =
  return ok(($m.data.hash).toBytes())

proc resolveAddress*(tAddress: string) : TransportAddress =
  ## resolve "hostname:port" or "ip:port" to TransportAddress
  resolveTAddress(tAddress)[0]
  # let ma = ta.mapIt(MultiAddress.init(it).tryGet())[0]
  # echo "resolveAddress: ", ta, ma
  # ma

proc reqDecode(req: seq[byte]): reqMessage = 
  let
    msgId = req[0].int
    row = req[1].int + (req[2].int shl 8)
    col = req[3].int + (req[4].int shl 8)
    tout = req[5]
  echo "request arrived:", (msgId, row, col, tout)
  reqMessage(msgId: msgId, row: row, col: col, tout: tout)

proc reqEncode(req: reqMessage): seq[byte] =
  @[req.msgId.byte, req.row.byte, (req.row shr 8).byte, req.col.byte, (req.col shr 8).byte, req.tout.byte]

type
  connMessage* = ref object
    direction*: byte
    custody*: byte

proc connEncode(conn: connMessage): seq[byte] =
  @[conn.direction, conn.custody]

proc connDecode(msg: seq[byte]): connMessage = 
  connMessage(direction: msg[0], custody: msg[1])

var reqId: int
proc request*(n: Network, peerId: NetworkPeerId, req: reqMessage): Future[respMessage] {.async.} =
  # send request
  let
    myReqId = reqId
  reqId += 1  
  echo "sending request ", myReqId, " to ", peerId.address
  await n.udpTransport.sendTo(peerId.address, @[0.byte, myReqId.byte] & reqEncode(req))

  # wait response
  let
    f = newFuture[void]()
  n.reqs[myReqId] = f
  await f
  echo "response arrived to ", myReqId

proc init*(reqHandler: ReqHandler, custody: byte) : Future[Network] {.async.} = 
  proc handler(stream: Connection, proto: string) {.async.} =
        let
          msg = reqDecode(await stream.readLp(6))
          resp = await reqHandler(msg) #, stream.peerId)
        if resp.isSome:
          await stream.writeLp([resp.get().code])
        await stream.close()

  var reqs = newTable[int, Future[void]]()
  var peers = newTable[TransportAddress, NetworkPeerId]()

  proc udpHandler(transp: DatagramTransport,
               raddr: TransportAddress): Future[void] {.async: (raises: []).} =
      try:
        let pbytes = transp.getMessage()

        if pbytes[0] == 0: # request
          # request arrived
          let reqId = pbytes[1]
          let req = reqDecode(pbytes[2..^1])
          echo "received", pbytes
          let resp = await reqHandler(req)
          if resp.isSome:
            await transp.sendTo(raddr, @[1.byte, reqId] & @[resp.get().code])

        elif pbytes[0] == 1: # response
          let reqId = pbytes[1]
          # response arrived. Search for request and notify
          echo "response arrived ", reqId, " from ", raddr
          reqs[reqId.int].complete()

        elif pbytes[0] == 2: # connect
          let msg = connDecode(pbytes[1..^1])
          echo "connect arrived from ", raddr, " with custody ", msg.custody
          echo "peers len:", peers.len
          if peers.hasKey(raddr):
            # if already known, update custody info
            peers[raddr].custody = msg.custody
          else:
            # first seen
            peers[raddr] = NetworkPeerId(address: raddr, custody: custody)

          if msg.direction == 0:
            echo "sending reverse connect"
            let msg2 = connMessage(direction: 1, custody: custody)
            await transp.sendTo(raddr, @[2.byte] & connEncode(msg2)) 

        else:
          echo "ERROR: wrong message type"

      except CatchableError as exc:
        echo "exception ", reqId
        raiseAssert exc.msg

  let
    rng = libp2p.newRng()
    address = initTAddress("0.0.0.0:5000")
    switch =
      SwitchBuilder
        .new()
        .withAgentVersion(getEnv("CUSTODY"))
        .withAddress(MultiAddress.init(address).tryGet())
        .withRng(rng)
        #.withYamux()
        .withMplex()
        .withMaxConnections(10000)
        .withTcpTransport(flags = {ServerFlags.TcpNoDelay})
        #.withPlainText()
        .withNoise()
        .build()
    gossipSub = GossipSub.init(
      switch = switch,
      # triggerSelf = true,
      msgIdProvider = msgIdProvider,
      verifySignature = false,
      anonymize = true,
      )
    #pingProtocol = Ping.new(rng=rng)
    reqProto = ReqProto.new(handler)

  gossipSub.parameters.floodPublish = false
  #gossipSub.parameters.lazyPushThreshold = 1_000_000_000
  #gossipSub.parameters.lazyPushThreshold = 0
  gossipSub.parameters.opportunisticGraftThreshold = -10000
  gossipSub.parameters.heartbeatInterval = 700.milliseconds
  gossipSub.parameters.pruneBackoff = 3.seconds
  gossipSub.parameters.gossipFactor = 0.05
  gossipSub.parameters.d = 8
  gossipSub.parameters.dLow = 6
  gossipSub.parameters.dHigh = 12
  gossipSub.parameters.dScore = 6
  gossipSub.parameters.dOut = 6 div 2
  gossipSub.parameters.dLazy = 6
  gossipSub.topicParams["test"] = TopicParams(
    topicWeight: 1,
    firstMessageDeliveriesWeight: 1,
    firstMessageDeliveriesCap: 30,
    firstMessageDeliveriesDecay: 0.9
  )

  switch.mount(gossipSub)
  #switch.mount(pingProtocol)
  switch.mount(reqProto)
  await switch.start()
  #TODO
  #defer: await switch.stop()

  # initialize UDP port
  let
    ta = initTAddress("0.0.0.0:5000")
    udpTransport = newDatagramTransport(udpHandler, local = ta)

  Network(
    switch: switch, gossipSub: gossipSub, 
    udpTransport:udpTransport, 
    reqs: reqs, 
    peers: peers,
    custody: custody)

proc connect*(n: Network, peerAddr: NetworkAddress): Future[NetworkPeerId] {.async}=
  echo "connect: ", peerAddr 
  let
    ma = MultiAddress.init(peerAddr).tryGet() 
    libp2pPeerId = await n.switch.connect(ma, allowUnknownPeerId=true)
    peerId = NetworkPeerId(peerId: libp2pPeerId, address: peerAddr, custody: 0)
  n.peers[peerAddr] = peerId

  echo "sending connect to ", peerAddr
  let msg = connMessage(direction: 0, custody: n.custody)
  discard n.udpTransport.sendTo(peerAddr, @[2.byte] & connEncode(msg)) 

  peerId  

proc publish*(n: Network,
                topic: string,
                data: seq[byte],
                maxCopies: int = int.high,
                shuffleDests = false): Future[int] {.async.} =

    await n.gossipSub.publish(topic, data, maxCopies, shuffleDests)

proc subscribe*(n: Network,
                topic: string,
                handler: TopicHandler) =

    n.gossipSub.subscribe(topic, handler)

proc addValidator*(n: Network,
                     topic: varargs[string],
                     hook: ValidatorHandler) =

    n.gossipSub.addValidator(topic, hook)

proc getPeerId*(n: Network) : NetworkPeerId =
    try:
      NetworkPeerId(
        peerId: n.switch.peerInfo.peerId, 
        address: n.udpTransport.localAddress,
        custody: n.custody)
    except CatchableError as exc:
      raiseAssert exc.msg

proc `$`*(x: NetworkPeerId) : string =
  $x.peerId

proc getNeighors*(n: Network, topic: string) : HashSet[PubSubPeer] =
    n.gossipSub.mesh.getOrDefault(topic)

proc getPeers*(n: Network): seq[NetworkPeerId] =
  # let
  #   # peers = switch.connectedPeers(Direction.Out) # we might need a bigger set
  #   peers = n.switch.peerStore[AddressBook].book
  # for p in peers.keys:
  #   result &= NetworkPeerId(peerId: p)
  n.peers.values.toSeq

proc getAddr*(n: Network) : auto =
  n.switch.peerInfo.addrs