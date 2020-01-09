package com.lightning.walletapp.lnutils

import spray.json._
import android.database.sqlite._
import com.lightning.walletapp.ln.PaymentInfo._
import com.lightning.walletapp.lnutils.ImplicitJsonFormats._
import com.lightning.walletapp.ln.Tools.{random, runAnd}
import com.lightning.walletapp.lnutils.olympus.CloudData
import android.content.Context
import scodec.bits.ByteVector
import android.net.Uri


object OlympusTable extends Table {
  val (table, identifier, url, data, auth, order, removable) = ("olympus", "identifier", "url", "data", "auth", "ord", "removable")
  val newSql = s"INSERT OR IGNORE INTO $table ($identifier, $url, $data, $auth, $order, $removable) VALUES (?, ?, ?, ?, ?, ?)"
  val updMetaSql = s"UPDATE $table SET $url = ?, $auth = ?, $order = ? WHERE $identifier = ?"
  val updDataSql = s"UPDATE $table SET $data = ? WHERE $identifier = ?"
  val selectAllSql = s"SELECT * FROM $table ORDER BY $order ASC"
  val killSql = s"DELETE FROM $table WHERE $identifier = ?"

  val createSql = s"""
    CREATE TABLE IF NOT EXISTS $table (
      $id INTEGER PRIMARY KEY AUTOINCREMENT, $identifier TEXT NOT NULL UNIQUE,
      $url STRING NOT NULL UNIQUE, $data STRING NOT NULL, $auth INTEGER NOT NULL,
      $order INTEGER NOT NULL, $removable INTEGER NOT NULL
    )"""
}

object OlympusLogTable extends Table {
  val (table, tokensUsed, explanation, stamp) = ("olympuslog", "tokensused", "explanation", "stamp")
  val newSql = s"INSERT INTO $table ($tokensUsed, $explanation, $stamp) VALUES (?, ?, ?)"
  val selectAllSql = s"SELECT * FROM $table ORDER BY $stamp DESC LIMIT 6"

  val createSql = s"""
    CREATE TABLE IF NOT EXISTS $table (
      $id INTEGER PRIMARY KEY AUTOINCREMENT, $tokensUsed INTEGER NOT NULL,
      $explanation STRING NOT NULL, $stamp INTEGER NOT NULL
    )"""
}

object ChannelTable extends Table {
  val Tuple3(table, identifier, data) = Tuple3("channel", "identifier", "data")
  val newSql = s"INSERT OR IGNORE INTO $table ($identifier, $data) VALUES (?, ?)"
  val updSql = s"UPDATE $table SET $data = ? WHERE $identifier = ?"
  val selectAllSql = s"SELECT * FROM $table ORDER BY $id DESC"
  val killSql = s"DELETE FROM $table WHERE $identifier = ?"

  val createSql = s"""
    CREATE TABLE IF NOT EXISTS $table (
      $id INTEGER PRIMARY KEY AUTOINCREMENT,
      $identifier TEXT NOT NULL UNIQUE,
      $data STRING NOT NULL
    )"""
}

object BadEntityTable extends Table {
  val (table, resId, expire, amount) = ("badentity", "resId", "expire", "amount")
  val newSql = s"INSERT OR IGNORE INTO $table ($resId, $expire, $amount) VALUES (?, ?, ?)"
  val selectSql = s"SELECT * FROM $table WHERE $expire > ? AND $amount <= ? LIMIT 320"
  val updSql = s"UPDATE $table SET $expire = ?, $amount = ? WHERE $resId = ?"

  val createSql = s"""
    CREATE TABLE IF NOT EXISTS $table (
      $id INTEGER PRIMARY KEY AUTOINCREMENT,
      $resId STRING NOT NULL UNIQUE,
      $expire INTEGER NOT NULL,
      $amount INTEGER NOT NULL
    );
    /* resId index is created automatically because this field is UNIQUE */
    CREATE INDEX IF NOT EXISTS idx1$table ON $table ($expire, $amount);
    COMMIT"""
}

object PaymentTable extends Table {
  val (search, table, pr, preimage, incoming, status, stamp, description, hash, firstMsat, lastMsat) = ("search", "payment", "pr", "preimage", "incoming", "status", "stamp", "description", "hash", "firstMsat", "lastMsat")
  val newSql = s"INSERT OR IGNORE INTO $table ($pr, $preimage, $incoming, $status, $stamp, $description, $hash, $firstMsat, $lastMsat, lastExpiry, chanId) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 0, 0)"
  val newVirtualSql = s"INSERT INTO $fts$table ($search, $hash) VALUES (?, ?)"

  // Selecting
  val selectSql = s"SELECT * FROM $table WHERE $hash = ?"
  val selectRecentSql = s"SELECT * FROM $table ORDER BY $id DESC LIMIT 48"
  val searchSql = s"SELECT * FROM $table WHERE $hash IN (SELECT $hash FROM $fts$table WHERE $search MATCH ? LIMIT 96)"

  // Updating, creating, removing
  val updOkOutgoingSql = s"UPDATE $table SET $status = $SUCCESS, $preimage = ? WHERE $hash = ?"
  val updOkIncomingSql = s"UPDATE $table SET $status = $SUCCESS, $firstMsat = ?, $stamp = ? WHERE $hash = ?"
  val updLastParamsOutgoingSql = s"UPDATE $table SET $status = $WAITING, $firstMsat = ?, $lastMsat = ? WHERE $hash = ?"
  // Broken in-flight payment may become fulfilled but then get overridden on restart unless we check for SUCCESS
  val updStatusSql = s"UPDATE $table SET $status = ? WHERE $hash = ? AND $status <> $SUCCESS"

  // Once incoming or outgoing payment is settled we can search it by various metadata
  val createVSql = s"CREATE VIRTUAL TABLE IF NOT EXISTS $fts$table USING $fts($search, $hash)"

  val createSql = s"""
    CREATE TABLE IF NOT EXISTS $table (
      $id INTEGER PRIMARY KEY AUTOINCREMENT, $pr STRING NOT NULL, $preimage STRING NOT NULL, $incoming INTEGER NOT NULL,
      $status INTEGER NOT NULL, $stamp INTEGER NOT NULL, $description STRING NOT NULL, $hash STRING NOT NULL UNIQUE,
      $firstMsat INTEGER NOT NULL, $lastMsat INTEGER NOT NULL, lastExpiry INTEGER NOT NULL, chanId STRING NOT NULL
    );
    /* hash index is created automatically because this field is UNIQUE */
    CREATE INDEX IF NOT EXISTS idx1$table ON $table ($status, $incoming, $stamp);
    COMMIT"""
}

object RevokedInfoTable extends Table {
  val (table, txId, chanId, myBalance, info, uploaded) = ("revokedinfo", "txid", "chanid", "mybalance", "info", "uploaded")
  val selectLocalSql = s"SELECT * FROM $table WHERE $chanId = ? AND $myBalance < ? AND $uploaded = 0 ORDER BY $myBalance ASC LIMIT 200"
  val newSql = s"INSERT INTO $table ($txId, $chanId, $myBalance, $info, $uploaded) VALUES (?, ?, ?, ?, 0)"
  val setUploadedSql = s"UPDATE $table SET $uploaded = 1 WHERE $txId = ?"
  val selectTxIdSql = s"SELECT * FROM $table WHERE $txId = ?"
  val killSql = s"DELETE FROM $table WHERE $chanId = ?"

  val createSql = s"""
    CREATE TABLE IF NOT EXISTS $table (
      $id INTEGER PRIMARY KEY AUTOINCREMENT, $txId STRING NOT NULL,
      $chanId STRING NOT NULL, $myBalance INTEGER NOT NULL,
      $info STRING NOT NULL, $uploaded INTEGER NOT NULL
    );

    CREATE INDEX IF NOT EXISTS idx2$table ON $table ($chanId, $myBalance, $uploaded);
    CREATE INDEX IF NOT EXISTS idx1$table ON $table ($txId);
    COMMIT"""
}

trait Table { val (id, fts) = "_id" -> "fts4" }
class LNOpenHelper(context: Context, name: String)
  extends SQLiteOpenHelper(context, name, null, 17) {

  val base = getWritableDatabase
  val asString: Any => String = {
    case alreadyString: String => alreadyString
    case byteVec: ByteVector => byteVec.toHex
    case otherwise => otherwise.toString
  }

  def change(sql: String, params: Any*) = base.execSQL(sql, params.map(asString).toArray)
  def select(sql: String, params: Any*) = base.rawQuery(sql, params.map(asString).toArray)
  def sqlPath(tbl: String) = Uri parse s"sqlite://com.lightning.wallet/table/$tbl"

  def txWrap(run: => Unit) = try {
    runAnd(base.beginTransaction)(run)
    base.setTransactionSuccessful
  } finally base.endTransaction

  def onCreate(dbs: SQLiteDatabase) = {
    dbs execSQL RevokedInfoTable.createSql
    dbs execSQL BadEntityTable.createSql
    dbs execSQL PaymentTable.createVSql
    dbs execSQL PaymentTable.createSql
    dbs execSQL ChannelTable.createSql

    dbs execSQL OlympusLogTable.createSql
    dbs execSQL OlympusTable.createSql
    populateOlympus(dbs)
  }

  def onUpgrade(dbs: SQLiteDatabase, v0: Int, v1: Int) = {
    dbs execSQL s"DROP TABLE IF EXISTS ${OlympusTable.table}"
    dbs execSQL OlympusTable.createSql
    populateOlympus(dbs)
  }

  def populateOlympus(dbs: SQLiteDatabase) = {
    val emptyData = CloudData(info = None, tokens = Vector.empty, acts = Vector.empty).toJson.toString
    val dev: Array[AnyRef] = Array("test-server-1", "https://107.172.253.20:9203", emptyData, "1", "0", "0")
    dbs.execSQL(OlympusTable.newSql, dev)
  }
}