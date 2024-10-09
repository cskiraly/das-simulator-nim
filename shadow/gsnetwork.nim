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
    switch*: Switch
    gossipSub: GossipSub
    udpTransport: DatagramTransport
  NetworkMessage* = Message
  NetworkAddress* = MultiAddress
  NetworkPeerId* = ref object
    peerId: PeerId
  reqMessage* = ref object
    msgId*: int
    row*: int
    col*: int
    tout*: byte
  respMessage* = ref object
    code*: byte

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
  parseInt(n.switch.peerStore[AgentBook][peerId.peerId])

proc msgIdProvider(m: Message): Result[MessageId, ValidationResult] =
  return ok(($m.data.hash).toBytes())

proc resolveAddress*(tAddress: string) : MultiAddress =
  resolveTAddress(tAddress).mapIt(MultiAddress.init(it).tryGet())[0]

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

proc request*(n: Network, peerId: NetworkPeerId, req: reqMessage): Future[respMessage] {.async.} =
  # send request
  let conn = await n.switch.dial(peerId.peerId, gsnetwork.ReqCodec)
  await conn.writeLp(reqEncode(req))
  # wait response
  let resp = await conn.readLp(1) #TODO: add timeout here
  respMessage(code: resp[0])

#proc request2*(n: Network, peerId: PeerId, req: reqMessage): Future[respMessage] {.async.} =
  # send request
  #n.udpTransport.sendTo(raddr, 0.byte & reqEncode(req))
  # wait response

proc init*(reqHandler: auto) : Future[Network] {.async.} = 
  proc handler(stream: Connection, proto: string) {.async.} =
        let
          msg = reqDecode(await stream.readLp(6))
          resp = await reqHandler(msg) #, stream.peerId)
        if resp.isSome:
          await stream.writeLp([resp.get().code])
        await stream.close()

  proc udpHandler(transp: DatagramTransport,
               raddr: TransportAddress): Future[void] {.async: (raises: []).} =
      try:
        let
          pbytes = transp.getMessage()
        if pbytes[0] == 0:
          # request arrived
          let req = reqDecode(pbytes[1..^1])
          echo "received", pbytes
          let resp = await reqHandler(req)
          if resp.isSome:
            await transp.sendTo(raddr, @[resp.get().code])
        else:
          # response arrived. Search for request and notify
          echo "response arrived"
      except CatchableError as exc:
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

  Network(switch: switch, gossipSub: gossipSub, udpTransport:udpTransport)

method connect*(n: Network, peerAddr: NetworkAddress): Future[NetworkPeerId] {.async}=
  let 
    peerId = await n.switch.connect(peerAddr, allowUnknownPeerId=true)
  NetworkPeerId(peerId: peerId)

method publish*(n: Network,
                topic: string,
                data: seq[byte],
                maxCopies: int = int.high,
                shuffleDests = false): Future[int] {.async.} =

    await n.gossipSub.publish(topic, data, maxCopies, shuffleDests)

method subscribe*(n: Network,
                topic: string,
                handler: TopicHandler) {.public, raises: [].} =

    n.gossipSub.subscribe(topic, handler)

method addValidator*(n: Network,
                     topic: varargs[string],
                     hook: ValidatorHandler) {.base, public, gcsafe, raises: [].} =

    n.gossipSub.addValidator(topic, hook)

method getPeerId*(n: Network) : NetworkPeerId {.raises: [].} =
    NetworkPeerId(peerId: n.gossipSub.switch.peerInfo.peerId)

method getNeighors*(n: Network, topic: string) : auto {.raises: [].} =
    n.gossipSub.mesh.getOrDefault(topic)

method getPeers*(n: Network): seq[NetworkPeerId] {.raises: [].} =
  let
    peers = n.switch.peerStore[AddressBook].book
  for p in peers.keys:
    result &= NetworkPeerId(peerId: p)
