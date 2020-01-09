package com.lightning.walletapp

import R.string._
import org.bitcoinj.core._
import scala.concurrent.duration._
import com.lightning.walletapp.ln._
import scala.collection.JavaConverters._
import com.lightning.walletapp.ln.Tools._
import com.lightning.walletapp.lnutils.ImplicitConversions._

import scala.util.{Success, Try}
import scodec.bits.{BitVector, ByteVector}
import android.content.{ClipboardManager, Context, Intent}
import com.lightning.walletapp.Utils.{walletFileName, chainFileName}
import com.lightning.walletapp.ln.wire.{NodeAnnouncement, NodeAddress}
import android.app.{Application, NotificationChannel, NotificationManager}
import com.lightning.walletapp.ln.crypto.Sphinx.{emptyOnionPacket, PacketAndSecrets}
import com.lightning.walletapp.ln.wire.LightningMessageCodecs.nodeaddress
import com.lightning.walletapp.lnutils.JsonHttpUtils.ioQueue
import com.lightning.walletapp.lnutils.olympus.OlympusWrap
import concurrent.ExecutionContext.Implicits.global
import com.lightning.walletapp.helper.AwaitService
import java.util.concurrent.TimeUnit.MILLISECONDS
import com.google.common.util.concurrent.Service
import android.support.v7.app.AppCompatDelegate
import org.bitcoinj.wallet.KeyChain.KeyPurpose
import org.bitcoinj.net.discovery.DnsDiscovery
import org.bitcoinj.wallet.Wallet.BalanceType
import java.util.Collections.singletonList
import com.lightning.walletapp.ln.LNParams
import fr.acinq.bitcoin.Crypto.PublicKey
import org.bitcoinj.wallet.SendRequest
import org.bitcoinj.uri.BitcoinURI
import scala.concurrent.Future
import android.widget.Toast
import android.os.Build
import java.io.File


class WalletApp extends Application { me =>
  Utils.appReference = me

  lazy val params = org.bitcoinj.params.TestNet3Params.get
  lazy val foregroundServiceIntent = new Intent(me, AwaitService.classof)
  lazy val prefs = getSharedPreferences("prefs", Context.MODE_PRIVATE)
  lazy val walletFile = new File(getFilesDir, walletFileName)
  lazy val chainFile = new File(getFilesDir, chainFileName)
  var kit: WalletKit = _

  lazy val plur = getString(lang) match {
    case "eng" | "esp" => (opts: Array[String], num: Long) => if (num == 1) opts(1) else opts(2)
    case "chn" | "jpn" => (phraseOptions: Array[String], num: Long) => phraseOptions(1)
    case "rus" | "ukr" => (phraseOptions: Array[String], num: Long) =>

      val reminder100 = num % 100
      val reminder10 = reminder100 % 10
      if (reminder100 > 10 & reminder100 < 20) phraseOptions(3)
      else if (reminder10 > 1 & reminder10 < 5) phraseOptions(2)
      else if (reminder10 == 1) phraseOptions(1)
      else phraseOptions(3)
  }

  // Various utilities

  def quickToast(code: Int): Unit = quickToast(me getString code)
  def quickToast(msg: CharSequence): Unit = Toast.makeText(me, msg, Toast.LENGTH_SHORT).show
  def clipboardManager = getSystemService(Context.CLIPBOARD_SERVICE).asInstanceOf[ClipboardManager]
  def plur1OrZero(opts: Array[String], num: Long) = if (num > 0) plur(opts, num).format(num) else opts(0)
  def getBufferUnsafe = clipboardManager.getPrimaryClip.getItemAt(0).getText.toString
  def notMixedCase(s: String) = s.toLowerCase == s || s.toUpperCase == s

  def isAlive =
    if (null == kit || null == LNParams.olympusWrap || null == LNParams.db || null == LNParams.keys) false
    else kit.state match { case Service.State.STARTING | Service.State.RUNNING => true case _ => false }

  override def onCreate = wrap(super.onCreate) {
    // These cannot be lazy vals because values may change
    Utils.fiatCode = prefs.getString(AbstractKit.FIAT_TYPE, "usd")
    Utils.denom = Utils denoms prefs.getInt(AbstractKit.DENOM_TYPE, 0)
    AppCompatDelegate.setDefaultNightMode(AppCompatDelegate.MODE_NIGHT_FOLLOW_SYSTEM)

    if (Build.VERSION.SDK_INT > Build.VERSION_CODES.N_MR1) {
      val importanceLevel = NotificationManager.IMPORTANCE_DEFAULT
      val srvChan = new NotificationChannel(AwaitService.CHANNEL_ID, "NC", importanceLevel)
      me getSystemService classOf[NotificationManager] createNotificationChannel srvChan
    }
  }

  def mkNodeAnnouncement(id: PublicKey, nodeAddress: NodeAddress, alias: String) =
    NodeAnnouncement(sign(fr.acinq.bitcoin.Protocol.Zeroes, randomPrivKey), features = ByteVector.empty,
      timestamp = 0L, nodeId = id, (-128, -128, -128), alias take 16, addresses = nodeAddress :: Nil)

  def emptyRD(pr: PaymentRequest, firstMsat: Long) =
    RoutingData(pr, routes = Vector.empty, usedRoute = Vector.empty, PacketAndSecrets(emptyOnionPacket, Vector.empty),
      firstMsat, lastMsat = 0L /* must be zero at start, will be saved this way and later updated once route is known */,
      lastExpiry = 0L, callsLeft = 4, action = None, fromHostedOnly = false)

  object TransData {
    var value: Any = new String
    private[this] val prefixes = PaymentRequest.prefixes.values mkString "|"
    private[this] val lnUrl = s"(?im).*?(lnurl)([0-9]{1,}[a-z0-9]+){1}".r.unanchored
    private[this] val lnPayReq = s"(?im).*?($prefixes)([0-9]{1,}[a-z0-9]+){1}".r.unanchored
    private[this] val shortNodeLink = "([a-fA-F0-9]{66})@([a-zA-Z0-9:\\.\\-_]+)".r.unanchored
    val nodeLink = "([a-fA-F0-9]{66})@([a-zA-Z0-9:\\.\\-_]+):([0-9]+)".r.unanchored

    case object RetainValue
    type Checker = PartialFunction[Any, Any]
    def checkAndMaybeErase(processAndCheck: Checker): Unit =
      if (processAndCheck(value) != RetainValue) value = null

    def bitcoinUri(bitcoinUriLink: String) = {
      val uri = new BitcoinURI(params, bitcoinUriLink)
      require(null != uri.getAddress, "No address detected")
      uri
    }

    def toBitcoinUri(addr: String) = bitcoinUri(s"bitcoin:$addr")
    def recordValue(rawInputText: String) = value = parse(rawInputText)
    def parse(rawInputTextToParse: String) = rawInputTextToParse take 2880 match {
      case bitcoinUriLink if bitcoinUriLink startsWith "bitcoin" => bitcoinUri(bitcoinUriLink)
      case bitcoinUriLink if bitcoinUriLink startsWith "BITCOIN" => bitcoinUri(bitcoinUriLink.toLowerCase)
      case nodeLink(key, host, port) => mkNodeAnnouncement(PublicKey(ByteVector fromValidHex key), NodeAddress.fromParts(host, port.toInt), host)
      case shortNodeLink(key, host) => mkNodeAnnouncement(PublicKey(ByteVector fromValidHex key), NodeAddress.fromParts(host, port = 9735), host)
      case lnPayReq(prefix, data) => PaymentRequest.read(s"$prefix$data")
      case lnUrl(prefix, data) => LNUrl.fromBech32(s"$prefix$data")
      case _ => toBitcoinUri(rawInputTextToParse)
    }
  }

  abstract class WalletKit extends AbstractKit {
    def currentAddress = wallet currentAddress KeyPurpose.RECEIVE_FUNDS
    def conf0Balance = wallet getBalance BalanceType.ESTIMATED_SPENDABLE // Returns all utxos
    def blockSend(txj: Transaction) = peerGroup.broadcastTransaction(txj, 1).broadcast.get
    def shutDown = none

    def trustedNodeTry = Try(nodeaddress.decode(BitVector fromValidHex wallet.getDescription).require.value)
    def fundingPubScript(some: HasNormalCommits) = singletonList(some.commitments.commitInput.txOut.publicKeyScript: org.bitcoinj.script.Script)
    def closingPubKeyScripts(cd: ClosingData) = cd.commitTxs.flatMap(_.txOut).map(_.publicKeyScript: org.bitcoinj.script.Script).asJava
    def useCheckPoints(time: Long) = CheckpointManager.checkpoint(params, getAssets open "checkpoints-testnet.txt", store, time)

    def sign(unsigned: SendRequest) = {
      // Create a tx ready for broadcast
      wallet finalizeReadyTx unsigned
      unsigned.tx.verify
      unsigned
    }

    def setupAndStartDownload = {
      wallet.allowSpendingUnconfirmedTransactions
      wallet.autosaveToFile(walletFile, 1000, MILLISECONDS, null)
      wallet.addCoinsSentEventListener(ChannelManager.chainEventsListener)
      wallet.addCoinsReceivedEventListener(ChannelManager.chainEventsListener)

      Future {
        trustedNodeTry match {
          case Success(nodeAddress) =>
            val isa = NodeAddress.toInetSocketAddress(nodeAddress)
            val trusted = new PeerAddress(params, isa.getAddress, isa.getPort)
            peerGroup.addAddress(trusted)

          case _ =>
            val discovery = new DnsDiscovery(params)
            peerGroup.addPeerDiscovery(discovery)
            peerGroup.setMaxConnections(5)
        }
      }

      peerGroup.setMinRequiredProtocolVersion(70015)
      peerGroup.setDownloadTxDependencies(0)
      peerGroup.setPingIntervalMsec(10000)
      peerGroup.addWallet(wallet)

      for {
        _ <- ioQueue delay 20.seconds
        offlineChannel <- ChannelManager.all
        if offlineChannel.state != Channel.CLOSING && offlineChannel.permanentOffline // This channel is not closing and was never connected since an app has started
        if offlineChannel.data.announce.addresses.headOption.forall(_.canBeUpdatedIfOffline) // Relevant for TOR channels: do not automatically convert them to clearnet!
        Vector(ann1 \ _, _*) <- LNParams.olympusWrap findNodes offlineChannel.data.announce.nodeId.toString // No `retry` wrapper because this gives harmless `Obs.empty` on error
      } offlineChannel process ann1

      ChannelManager.listeners += new ChannelManagerListener {
        override def incomingReceived(amt: Long, info: PaymentInfo) =
          stopService(foregroundServiceIntent)
      }

      startBlocksDownload(ChannelManager.chainEventsListener)
      // Try to clear act leftovers if no channels are left
      LNParams.olympusWrap tellClouds OlympusWrap.CMDStart
      ChannelManager.initConnect
    }
  }
}