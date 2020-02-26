package com.lightning.walletapp

import android.database.{ContentObserver, Cursor}
import com.lightning.walletapp.FragPayMarket._
import android.view.{LayoutInflater, View, ViewGroup}
import com.arlib.floatingsearchview.FloatingSearchView.OnQueryChangeListener
import com.arlib.floatingsearchview.FloatingSearchView
import com.lightning.walletapp.Utils._
import android.support.v4.app.Fragment
import android.widget.{BaseAdapter, GridView, ImageView, TextView}
import android.os.{Bundle, Handler}
import android.support.v4.app.LoaderManager.LoaderCallbacks
import android.support.v4.content.Loader
import android.transition.TransitionManager
import com.lightning.walletapp.helper.{ReactLoader, RichCursor}
import com.lightning.walletapp.ln.LNParams._
import com.lightning.walletapp.ln.Tools._
import com.lightning.walletapp.lnutils.ImplicitConversions._
import com.lightning.walletapp.lnutils.IconGetter._
import com.lightning.walletapp.lnutils.{PayMarketTable, PayMarketWrap}


object FragPayMarket {
  var worker: FragPayMarketWorker = _
}

class FragPayMarket extends Fragment {
  override def onCreateView(inf: LayoutInflater, viewGroup: ViewGroup, bundle: Bundle) = inf.inflate(R.layout.frag_view_pager_paymarket, viewGroup, false)
  override def onViewCreated(view: View, state: Bundle) = if (app.isAlive) worker = new FragPayMarketWorker(getActivity.asInstanceOf[WalletActivity], view)
}

class FragPayMarketWorker(val host: WalletActivity, frag: View) extends HumanTimeDisplay { me =>
  val paySearch = frag.findViewById(R.id.paySearch).asInstanceOf[FloatingSearchView]
  val gridView = frag.findViewById(R.id.gridView).asInstanceOf[GridView]
  var allPayLinks = Vector.empty[PayLinkInfo]

  val adapter = new BaseAdapter {
    def getCount = allPayLinks.size
    def getItemId(linkPosition: Int) = linkPosition
    def getItem(position: Int) = allPayLinks(position)
    def getView(position: Int, savedView: View, parent: ViewGroup) = {
      val card = if (null == savedView) host.getLayoutInflater.inflate(R.layout.card_pay_link, null) else savedView
      val holder = if (null == card.getTag) ViewHolder(card) else card.getTag.asInstanceOf[ViewHolder]
      holder fillView getItem(position)
      card
    }
  }

  val loaderCallbacks = new LoaderCallbacks[Cursor] {
    def onCreateLoader(id: Int, bn: Bundle) = new ReactLoader[PayLinkInfo](host) {
      val consume = (payLinks: PayLinkInfoVec) => runAnd(allPayLinks = payLinks)(updPayLinksList.run)
      def getCursor = if (paySearch.getQuery.isEmpty) PayMarketWrap.byRecent else PayMarketWrap.byQuery(paySearch.getQuery)
      def createItem(rc: RichCursor) = PayMarketWrap.toLinkInfo(rc)
    }

    type LoaderCursor = Loader[Cursor]
    type PayLinkInfoVec = Vector[PayLinkInfo]
    def onLoaderReset(loaderCursor: LoaderCursor) = none
    def onLoadFinished(loaderCursor: LoaderCursor, c: Cursor) = none
  }

  case class ViewHolder(view: View) {
    val image = view.findViewById(R.id.image).asInstanceOf[ImageView]
    val domainName = view.findViewById(R.id.domainName).asInstanceOf[TextView]
    val textMetadata = view.findViewById(R.id.textMetadata).asInstanceOf[TextView]
    val lastAttempt = view.findViewById(R.id.lastAttempt).asInstanceOf[TextView]
    view setTag this

    def fillView(info: PayLinkInfo) = {
      val msat = denom parsedWithSign info.lastMsat
      val date = me time new java.util.Date(info.lastDate)

      info.bitmap map image.setImageBitmap
      image setVisibility viewMap(info.bitmap.isSuccess)
      lastAttempt setText s"$date &#8226; $msat".html
      domainName setText info.lnurl.uri.getHost
      textMetadata setText info.text
    }
  }

  def updPayLinksList = host UITask {
    TransitionManager beginDelayedTransition gridView
    adapter.notifyDataSetChanged
  }

  def reload = android.support.v4.app.LoaderManager.getInstance(host).restartLoader(2, null, loaderCallbacks).forceLoad
  val observer = new ContentObserver(new Handler) { override def onChange(askedFromSelf: Boolean) = if (!askedFromSelf) reload }
  paySearch setOnQueryChangeListener new OnQueryChangeListener { def onSearchTextChanged(q0: String, q1: String) = reload }
  host.getContentResolver.registerContentObserver(db sqlPath PayMarketTable.table, true, observer)
  gridView setNumColumns math.round(scrWidth / 2.4).toInt
  gridView setAdapter adapter
  reload
}