import stew/endians2, stew/byteutils, tables, strutils, os
import vendor/nim-libp2p/libp2p
import chronos
import random # need since rng leads to "Error: internal error: could not find env param for segmentItRandom"
import sequtils, hashes, math, metrics
from times import getTime, toUnix, fromUnix, `-`, initTime, `$`, inMilliseconds, Duration
from nativesockets import getHostname

import gsnetwork

proc shadowPeerId2peerAddr(i: int): MultiAddress =
  ## convert Shadow node ID to address
  let tAddress = "peer" & $i & ":5000"
  resolveTAddress(tAddress).mapIt(MultiAddress.init(it).tryGet())[0]

proc peerAddr2rng(peerAddr: MultiAddress, usecase: auto): Rand =
  ## get an RNG for a given peer
  let seed =  hash((peerAddr, usecase))
  initRand(seed)

proc peerId2rng(peerId: PeerId, usecase: auto): Rand =
  ## get an RNG for a given peer
  let seed =  hash((peerId, usecase))
  initRand(seed)

proc main {.async.} =
  # make sure random is random
  randomize()

  let
    custodyRows = parseInt(getEnv("CUSTODY"))   # rows to custody (=topics to sbscribe)
    custodyCols = parseInt(getEnv("CUSTODY"))
  const
    blocksize = 2^21  # size of DAS block, before EC, in bytes
    numRows = 128      # number of Rows after EC
    numRowsK = 64     # number of Rows before EC
    numCols = 128
    numColsK = 64
    sendRows = true   # whether the publisher send out on row topics
    sendCols = true
    crossForward = true   # whether to relay received segments in the other dimension (row->col, col->row)
    publisherMaxCopies = 1  # how many copies of each segment to send out (see shufflepeers as well)
    publisherShufflePeers = true # how to select peers to send to. false: always the same; true: randomize
    publisherSendInRandomOrder = true  # whether to radomize segment order when publishing
    publisherSendRowCount = numRows # numRows: send whole row; numRowsK: send only half row
    publisherSendColCount = numCols
    repairOnTheFly = true # whether to repar as soon as a whole K arrived (both row and column)
    repairForward = false # whether to forward repaired chunks on the same line
    repairCrossForward = true # wheher to forward repaired segments on the other dimension

    sampleCount = 71

    msgCount = 3

    printGossipSubStats = false
  let
    interest = numRows * custodyCols + (numCols-custodyCols) * custodyRows
  let
    hostname = getHostname()
    myId = parseInt(hostname[4..^1])
    #publisherCount = client.param(int, "publisher_count")
    publisherCount = 1
    isPublisher = myId <= publisherCount
    #isAttacker = (not isPublisher) and myId - publisherCount <= client.param(int, "attacker_count")
    isAttacker = false
    rng = libp2p.newRng()
    #randCountry = rng.rand(distribCumSummed[^1])
    #country = distribCumSummed.find(distribCumSummed.filterIt(it >= randCountry)[0])

  var messagesChunks = initTable[int, CountTable[(int, int)]]()
  var messagesChunkCount = initCountTable[int]()

  ##
  # ReqResp Protocol
  ##    
  var rx: Table[(int, int, int), seq[Future[void]]] = initTable[(int, int, int), seq[Future[void]]]()
  # create handler for incoming connection

  proc reqHandler(m: reqMessage, src: PeerId): Future[Option[respMessage]] {.async.} =
        let reqDbg = (m.msgId, src, m.row, m.col, m.tout)
        if messagesChunks.hasKey(m.msgId) and messagesChunks[m.msgId][(m.row,m.col)] >= 1:
          echo "already heaving", reqDbg
        else:
          echo "waiting for", reqDbg
          let f = newFuture[void]()
          rx.mgetOrPut((m.msgId, m.row, m.col), newSeq[Future[void]]()).add(f)
          if await f.withTimeout(m.tout.seconds):
            rx[(m.msgId, m.row, m.col)].delete(rx[(m.msgId, m.row, m.col)].find(f))
          else:
            echo "tout expired for", reqDbg
            return

        echo "responding for", reqDbg
        some(respMessage(code: 1.byte)) # TODO: send segment

  let netw = await gsnetwork.init(reqHandler)

  proc peerToRows(peerId: PeerId) : seq[int] =
    let peerCustody =
      try:
        parseInt(netw.switch.peerStore[AgentBook][peerId])
      except CatchableError as exc:
        parseInt(getEnv("CUSTODY"))
    result = toSeq(0..<numRows)
    if not isPublisher:
      var rng = peerId2rng(peerId, "rows")
      rng.shuffle(result)
      result = result[0..<peerCustody]

  proc peerToCols(peerId: PeerId) : seq[int] =
    let peerCustody =
      try:
        parseInt(netw.switch.peerStore[AgentBook][peerId])
      except CatchableError as exc:
        parseInt(getEnv("CUSTODY"))
    result = toSeq(0..<numCols)
    if not isPublisher:
      var rng = peerId2rng(peerId, "cols")
      rng.shuffle(result)
      result = result[0..<peerCustody]

  var rows = peerToRows(netw.getPeerId())
  var cols = peerToCols(netw.getPeerId())

  proc dasTopicR(row: int) : string =
    "R" & $row

  proc dasTopicC(col: int) : string =
    "C" & $col

  proc isTopicR(topic: string) : bool =
    topic[0] == 'R'

  proc messageLatency(data: seq[byte]) : times.Duration =
    let
      sentMoment = nanoseconds(int64(uint64.fromBytesLE(data)))
      sentNanosecs = nanoseconds(sentMoment - seconds(sentMoment.seconds))
      sentDate = initTime(sentMoment.seconds, sentNanosecs)
    result = getTime() - sentDate

  proc messageHandler(topic: string, data: seq[byte]) {.async.} =
    let
      sentUint = uint64.fromBytesLE(data)
      msgId = data[10].int
      row = data[12].int + (data[13].int shl 8)
      col = data[14].int + (data[15].int shl 8)
      roc = topic.isTopicR # Row or Column

    # warm-up
    if sentUint < 1000000: return
    #if isAttacker: return

    if not messagesChunks.hasKey(msgId):
      messagesChunks[msgId] = initCountTable[(int, int)]()

    proc sendOnCol(col: int, data: seq[byte]) =
          var rocData = data
          rocData[16] = 1
          discard netw.publish(dasTopicC(int(col)), rocData)

    proc sendOnRow(row: int, data: seq[byte]) =
          var rocData = data
          rocData[16] = 0
          discard netw.publish(dasTopicR(int(row)), rocData)

    if crossForward:
      if roc:
        if int(col) in cols:
          #echo "crossing to col: ", col
          sendOnCol(col, data)
      else:
        if int(row) in rows:
          #echo "crossing to row: ", row
          sendOnRow(row, data)

    messagesChunks[msgId].inc((row,col))
    if messagesChunks[msgId][(row,col)] > 1:
      #echo sentUint, " DUP ms: ", messageLatency(data).inMilliseconds(), " r", row, "c", col
      return
    else:
      messagesChunkCount.inc(msgId)
      #echo sentUint, " ARR ms: ", messageLatency(data).inMilliseconds(), " r", row, "c", col, " ", messagesChunkCount[msgId], "/", interest

    # answer request if needed
    if rx.haskey((msgId, row, col)):
      echo "Answering requests for ", msgId, " r", row, "c", col
      for f in rx[(msgId, row, col)]:
        f.complete()

    proc hasInRow(row:int) : int =
      for i in 0 ..< numCols :
        if messagesChunks[msgId][(row, i)] >= 1:
          result += 1

    proc hasInCol(col:int) : int =
      for i in 0 ..< numRows :
        if messagesChunks[msgId][(i, col)] >= 1:
          result += 1

    if repairOnTheFly:
      if int(row) in rows:
        if hasInRow(row) >= numColsK:
          echo "Repairing r", row
          for i in 0 ..< numCols :
            if messagesChunks[msgId][(row, i)] == 0:
              messagesChunks[msgId][(row, i)] = 1
              messagesChunkCount.inc(msgId)
              if repairCrossForward:
                if int(col) in cols:
                  sendOnCol(col, data)
              if repairForward:
                sendOnRow(row, data)
              if rx.haskey((msgId, row, i)):
                echo "Answering requests for ", msgId, " r", row, "c", i
                for f in rx[(msgId, row, i)]:
                  f.complete()

      if int(col) in cols:
        if hasInCol(col) >= numRowsK:
          echo "Repairing c", col
          for i in 0 ..< numRows :
            if messagesChunks[msgId][(i, col)] == 0:
              messagesChunks[msgId][(i, col)] = 1
              messagesChunkCount.inc(msgId)
              if repairCrossForward:
                if int(row) in rows:
                  sendOnRow(row, data)
              if repairForward:
                sendOnCol(col, data)
              if rx.haskey((msgId, i, col)):
                echo "Answering requests for ", msgId, " r", i, "c", col
                for f in rx[(msgId, i, col)]:
                  f.complete()

    if messagesChunkCount[msgId] < interest: return

    echo msgId, " BLK ms: ", messageLatency(data).inMilliseconds(), " block arrived"
    echo msgId, " milliseconds: ", messageLatency(data).inMilliseconds()

  var
    startOfTest: Moment
    attackAfter = 10000.hours

  proc messageValidator(topic: string, msg: NetworkMessage): Future[ValidationResult] {.async.} =
    if isAttacker and Moment.now - startOfTest >= attackAfter:
      return ValidationResult.Ignore

    return ValidationResult.Accept

  for row in rows:
    netw.subscribe(dasTopicR(row), messageHandler)
    netw.addValidator([dasTopicR(row)], messageValidator)

  for col in cols:
    netw.subscribe(dasTopicC(col), messageHandler)
    netw.addValidator([dasTopicC(col)], messageValidator)

  echo "Listening on ", netw.switch.peerInfo.addrs
  echo myId, ", ", isPublisher, ", ", netw.switch.peerInfo.peerId

  var peersInfo = toSeq(1..parseInt(getEnv("PEERS")))
  rng.shuffle(peersInfo)

  # proc pinger(peerId: PeerId) {.async.} =
  #   try:
  #     await sleepAsync(20.seconds)
  #     while true:
  #       let stream = await netw.switch.dial(peerId, PingCodec)
  #       let delay = await pingProtocol.ping(stream)
  #       await stream.close()
  #       #echo delay
  #       await sleepAsync(delay)
  #   except:
  #     echo "Failed to ping"

  let connectTo = parseInt(getEnv("CONNECTTO"))
  proc connectToPeers(c: int) {.async.} =
    var connected = 0
    for peerInfo in peersInfo:
      if connected >= connectTo: break
      let peerAddr = shadowPeerId2peerAddr(peerInfo)
      try:
        let peerId = await netw.switch.connect(peerAddr, allowUnknownPeerId=true).wait(5.seconds)
        #asyncSpawn pinger(peerId)
        connected.inc()
      except CatchableError as exc:
        echo "Failed to dial", exc.msg

  discard connectToPeers(connectTo)

  #let
  #  maxMessageDelay = client.param(int, "max_message_delay")
  #  warmupMessages = client.param(int, "warmup_messages")
  #startOfTest = Moment.now() + milliseconds(warmupMessages * maxMessageDelay div 2)

  await sleepAsync(180.seconds)
  # echo "Mesh size: ", netw.getNeighors("test").len
  for row in rows:
    let topic = dasTopicR(row)
    echo "Mesh size ", topic, " ", netw.getNeighors(topic).len
  for col in cols:
    let topic = dasTopicC(col)
    echo "Mesh size ", topic, " ", netw.getNeighors(topic).len

  for msg in 0 ..< msgCount:#client.param(int, "message_count"):
    let startTime = getTime()
    if msg mod publisherCount == myId - 1:
    #if myId == 1:
      let nowInt = seconds(startTime.toUnix()) + nanoseconds(times.nanosecond(startTime))
      var nowBytes = @(toBytesLE(uint64(nowInt.nanoseconds))) & newSeq[byte](blocksize div (numRowsK*numColsK))
      echo "sending ", uint64(nowInt.nanoseconds)

      iterator segmentItRC() : (int, int) =
        for row in 0..<publisherSendRowCount:
          for col in 0..<publisherSendColCount:
            yield (row, col)

      iterator segmentIt() : (int, int) {.inline.} =
        type segmentIdx = tuple[row: int, col: int]
        var segments : seq[segmentIdx]
        for rc in segmentItRC():
            segments.add(rc)
        if publisherSendInRandomOrder:
          # rnd.shuffle(segments) # TODO: this leads to "Error: internal error: could not find env param"
          random.shuffle(segments)
        for rc in segments:
          yield rc

      for (row, col) in segmentIt():
          nowBytes[10] = byte(msg)
          nowBytes[12] = byte(row)
          nowBytes[13] = byte(row shr 8)
          nowBytes[14] = byte(col)
          nowBytes[15] = byte(col shr 8)
          echo "sending ", uint64(nowInt.nanoseconds), " r", row, "c", col
          if sendRows:
            nowBytes[16] = 0
            discard netw.publish(dasTopicR(row), nowBytes, publisherMaxCopies, publisherShufflePeers)
          if sendCols:
            nowBytes[16] = 1
            discard netw.publish(dasTopicC(col), nowBytes, publisherMaxCopies, publisherShufflePeers)
    else:
      ## start sampling

      let
        #peers = switch.connectedPeers(Direction.Out) # we might need a bigger set
        peers = netw.switch.peerStore[AddressBook].book
      echo "Peers:", peers
      var
        colPeers: array[numCols, HashSet[PeerId]]  #peers interested in a given column
        rowPeers: array[numRows, HashSet[PeerId]]

      for peerId in peers.keys:
        let
          cols = peerToCols(peerId)
          rows = peerToRows(peerId)
        for col in cols:
          colPeers[col].incl(peerId)
        for row in rows:
          rowPeers[row].incl(peerId)
      #echo "colPeers:", colPeers
      #echo "rowPeers:", rowPeers

      proc sampleOne(msg, row, col: int): Future[bool] {.async.} =
        # select peer
        var candidates = toSeq(rowPeers[row] + colPeers[col])
        random.shuffle(candidates)
        if candidates.len == 0:
          echo "Warning, not enoough peers for ", "r", row, "c", col
          #TODO: look for new peers
          return false
        for i, peerId in candidates.pairs:
          try:
            let
              tout = (if i == 0: 3 else: 1) # first 3 seconds, then 1
              req = [msg.byte, row.byte, (row shr 8).byte, col.byte, (col shr 8).byte, tout.byte]
              #peerId = await switch.connect(addrs[0], allowUnknownPeerId=true).wait(5.seconds)
              conn = await netw.switch.dial(peerId, gsnetwork.ReqCodec)
            try:
              echo "requesting:", ((getTime()-startTime).inMilliseconds(), i, peerId, msg, row, col)
              await conn.writeLp(req)
              let resp = await conn.readLp(1) #TODO: add timeout here
              echo "Received sample ", ((getTime()-startTime).inMilliseconds(), peerId, msg, row, col)
              return true
            except CatchableError as exc:
              echo "ReqResp error ", (getTime()-startTime).inMilliseconds(), exc.msg
          except CatchableError as exc:
            echo "Failed to dial: ", exc.msg

        #no one responded
        return false

      proc sampleMany(msg, sampleCount: int): Future[bool] {.async.} =
        var
          sampleR = toSeq(0..<numRows)
          sampleC = toSeq(0..<numCols)
        random.shuffle(sampleR)
        random.shuffle(sampleC)

        assert(numRows >= sampleCount)
        assert(numCols >= sampleCount)

        var sampling : seq[Future[bool]]
        for i in 0..<sampleCount:
          sampling.add(sampleOne(msg, sampleR[i], sampleC[i]))

        let 
          samplingResult = await allFinished(sampling)
          success = samplingResult.mapIt(it.read).count(true)
        echo "Sampling result:", ((getTime()-startTime).inMilliseconds(), success, sampleCount)
        if (success == sampleCount):
          echo msg,"-s", " milliseconds: ", (getTime()-startTime).inMilliseconds()

        return (success == sampleCount)

      discard sampleMany(msg, sampleCount)

    #wait for next slot
    await sleepAsync(12.seconds)

  #echo "BW: ", libp2p_protocols_bytes.value(labelValues=["/meshsub/1.1.0", "in"]) + libp2p_protocols_bytes.value(labelValues=["/meshsub/1.1.0", "out"])
  #echo "DUPS: ", libp2p_gossipsub_duplicate.value(), " / ", libp2p_gossipsub_received.value()

  when printGossipSubStats:
    #requires exporting counters from GossipSub.nim
    echo "statcounters: dup_during_validation ", libp2p_gossipsub_duplicate_during_validation.value(),
        "\tidontwant_saves ", libp2p_gossipsub_idontwant_saved_messages.value(),
        #"gossip optimization saves ", libp2p_gossipsub_saved_bytes.value(),
        "\tdup_received ", libp2p_gossipsub_duplicate.value(),
        "\tUnique_msg_received ", libp2p_gossipsub_received.value(),
        "\tStaggered_Saves ", libp2p_gossipsub_staggerSave.value(),
        "\tDontWant_IN_Stagger ", libp2p_gossipsub_staggerDontWantSave.value()
waitFor(main())
