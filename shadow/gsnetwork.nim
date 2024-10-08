import stew/endians2, stew/byteutils, tables, strutils, os
import vendor/nim-libp2p/libp2p, vendor/nim-libp2p/libp2p/protocols/pubsub/rpc/messages
import vendor/nim-libp2p/libp2p/muxers/mplex/lpchannel, vendor/nim-libp2p/libp2p/protocols/ping
import chronos
import random # need since rng leads to "Error: internal error: could not find env param for segmentItRandom"
import hashes
from times import getTime, toUnix, fromUnix, `-`, initTime, `$`, inMilliseconds, Duration
from nativesockets import getHostname

type
  Network = ref object
    switch*: Switch
    gossipSub: GossipSub
  NetworkMessage* = Message
  reqMessage* = ref object
    msgId*: int
    row*: int
    col*: int
    tout*: byte
  respMessage* = ref object
    code*: byte

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

proc msgIdProvider(m: Message): Result[MessageId, ValidationResult] =
  return ok(($m.data.hash).toBytes())

proc init*(reqHandler: auto) : Future[Network] {.async.} = 
  proc handler(stream: Connection, proto: string) {.async.} =
        let
          req = await stream.readLp(6)
          msgId = req[0].int
          row = req[1].int + (req[2].int shl 8)
          col = req[3].int + (req[4].int shl 8)
          tout = req[5]
          reqDbg = (msgId, stream.peerId, row, col, tout)
        echo "request arrived:", reqDbg
        let
          msg = reqMessage(msgId: msgId, row: row, col: col, tout: tout)
          resp = await reqHandler(msg, stream.peerId)
        if resp.isSome:
          await stream.writeLp([resp.get().code])
        await stream.close()

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
  Network(switch: switch, gossipSub: gossipSub)

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

method getPeerId*(n: Network) : PeerId {.raises: [].} =
    n.gossipSub.switch.peerInfo.peerId

method getNeighors*(n: Network, topic: string) : auto {.raises: [].} =
    n.gossipSub.mesh.getOrDefault(topic)