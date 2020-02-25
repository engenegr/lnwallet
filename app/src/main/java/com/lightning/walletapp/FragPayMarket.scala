package com.lightning.walletapp

import android.database.{ContentObserver, Cursor}
import com.lightning.walletapp.FragPayMarket._
import android.view.{LayoutInflater, View, ViewGroup}
import com.arlib.floatingsearchview.FloatingSearchView.OnQueryChangeListener
import com.arlib.floatingsearchview.FloatingSearchView
import com.lightning.walletapp.Utils.app
import android.support.v4.app.Fragment
import android.widget.{BaseAdapter, GridView}
import android.os.{Bundle, Handler}
import android.support.v4.app.LoaderManager.LoaderCallbacks
import android.support.v4.content.Loader
import com.lightning.walletapp.helper.{ReactLoader, RichCursor}
import com.lightning.walletapp.ln.LNParams._
import com.lightning.walletapp.ln.Tools._
import com.lightning.walletapp.lnutils.{PayMarketTable, PayMarketWrap}


object FragPayMarket {
  var worker: FragPayMarketWorker = _
}

class FragPayMarket extends Fragment {
  override def onCreateView(inf: LayoutInflater, viewGroup: ViewGroup, bundle: Bundle) = inf.inflate(R.layout.frag_view_pager_paymarket, viewGroup, false)
  override def onViewCreated(view: View, state: Bundle) = if (app.isAlive) worker = new FragPayMarketWorker(getActivity.asInstanceOf[WalletActivity], view)
}

class FragPayMarketWorker(val host: WalletActivity, frag: View) extends HumanTimeDisplay {
  val paySearch = frag.findViewById(R.id.paySearch).asInstanceOf[FloatingSearchView]
  val gridView = frag.findViewById(R.id.gridView).asInstanceOf[GridView]
  var allPayLinks = Vector.empty[PayLinkInfo]

  val adapter = new BaseAdapter {
    def getCount = allPayLinks.size
    def getItemId(linkPosition: Int) = linkPosition
    def getItem(position: Int) = allPayLinks(position)
    def getView(position: Int, savedView: View, parent: ViewGroup) = {
      val card = if (null == savedView) host.getLayoutInflater.inflate(R.layout.card_pay_link, null) else savedView
      card
    }
  }

  val loaderCallbacks = new LoaderCallbacks[Cursor] {
    def onCreateLoader(id: Int, bn: Bundle) = new ReactLoader[PayLinkInfo](host) {
      val consume = (payLinks: PayLinkInfoVec) => runAnd(allPayLinks = payLinks)(adapter.notifyDataSetChanged)
      def getCursor = if (paySearch.getQuery.isEmpty) PayMarketWrap.byRecent else PayMarketWrap.byQuery(paySearch.getQuery)
      def createItem(rc: RichCursor) = PayMarketWrap.toLinkInfo(rc)
    }

    type LoaderCursor = Loader[Cursor]
    type PayLinkInfoVec = Vector[PayLinkInfo]
    def onLoaderReset(loaderCursor: LoaderCursor) = none
    def onLoadFinished(loaderCursor: LoaderCursor, c: Cursor) = none
  }

  def reload = android.support.v4.app.LoaderManager.getInstance(host).restartLoader(2, null, loaderCallbacks).forceLoad
  val observer = new ContentObserver(new Handler) { override def onChange(askedFromSelf: Boolean) = if (!askedFromSelf) reload }
  paySearch setOnQueryChangeListener new OnQueryChangeListener { def onSearchTextChanged(q0: String, q1: String) = reload }
  host.getContentResolver.registerContentObserver(db sqlPath PayMarketTable.table, true, observer)
}