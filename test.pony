use "ponytest"
use "collections"
use "time"

class TestNotify is Notify
  let _env: Env

  new create(env: Env) =>
    _env = env

  fun failed(err: Error) =>
    _env.out.print(err.method() + " failed: " + err.string())

class DB
  let name: String  = "mypony"
  let _host: String = "localhost"
  let _user: String = "root"
  let _pass: String = ""

  fun connect(env: Env, db: (String | None)): MySQL ? =>
    let m = _parse_env(env)
    let host = try m("host")? else _host end
    let user = try m("user")? else _user end
    let pass = try m("pass")? else _pass end
    MySQL(TestNotify(env)).tcp(host, user, pass, db)?

  fun run(env: Env, query: String,
          params: Array[QueryParam] = Array[QueryParam](),
          f: ({(Result)} | None) = None) ? =>
    _run(None, env, query, params, f)?

  fun apply(env: Env, query: String,
            params: Array[QueryParam] = Array[QueryParam](),
            f: ({(Result) ?} | None) = None) ? =>
    _run(name, env, query, params, f)?

  fun _run(db: (String | None), env: Env, query: String,
           params: Array[QueryParam] = Array[QueryParam](),
           f: ({(Result) ?} | None) = None) ? =>
    with mysql = DB.connect(env, db)? do
      with stmt = mysql.prepare(query)? do
        let res = stmt.execute(params)?
        match f
        | let f': {(Result)} => f'(res)
        end
      end
    end

  fun _parse_env(env: Env): Map[String, String] =>
    let m = Map[String, String]
    for v in env.vars.values() do
      let a = v.split("=", 2)
      try
        match a(0)?
        | "MYPONY_HOST" => m("host") = a(1)?
        | "MYPONY_USER" => m("user") = a(1)?
        | "MYPONY_PASS" => m("pass") = a(1)?
        | "MYPONY_DB"   => m("db")   = a(1)?
        end
      end
    end
    m

actor Main is TestList
  new create(env: Env) =>
    try
      create_database(env)?
      create_table(env)?
      truncate_table(env)?
    end
    PonyTest(env, this)

  new make() => None

  fun tag create_database(env: Env) ? =>
    let query = "CREATE DATABASE IF NOT EXISTS " + DB.name
    DB.run(env, query)?

  fun tag create_table(env: Env) ? =>
    let query = "CREATE TABLE IF NOT EXISTS t (
      my_tiny    TINYINT,
      my_small   SMALLINT,
      my_int     INTEGER,
      my_bigint  BIGINT,

      my_utiny   TINYINT UNSIGNED,
      my_usmall  SMALLINT UNSIGNED,
      my_uint    INTEGER UNSIGNED,
      my_ubigint BIGINT UNSIGNED,

      my_string  VARCHAR(255),
      my_blob    BLOB,

      my_date    TIMESTAMP
    )"
    DB(env, query)?

  fun tag truncate_table(env: Env) ? =>
    let query = "TRUNCATE TABLE t"
    DB(env, query)?

  fun tag tests(test: PonyTest) =>
    test(_TestPing)
    test(_TestInsert)
    test(_TestOption)
    test(_TestNumResults)
    test(_TestFetchArray)
    test(_TestFetchMap)

class iso _TestPing is UnitTest
  fun name(): String => "mysql/ping"

  fun apply(h: TestHelper) ? =>
    let mysql = DB.connect(h.env, "mypony")?
    h.assert_true(mysql.ping())

class iso _TestInsert is UnitTest
  fun name(): String => "mysql/create_table"
  fun exclusion_group(): String => "mysql/with_inserted_data"

  fun ref apply(h: TestHelper) ? =>
    let query = "INSERT INTO t (
      my_tiny,
      my_small,
      my_int,
      my_bigint,

      my_utiny,
      my_usmall,
      my_uint,
      my_ubigint,

      my_string,
      my_blob,

      my_date
    ) VALUES (
      ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
    )"
    let my_tiny: QueryParam    = I8(1)
    let my_small: QueryParam   = I16(2)
    let my_int: QueryParam     = I32(3)
    let my_bigint: QueryParam  = I64(4)
    let my_utiny: QueryParam   = U8(5)
    let my_usmall: QueryParam  = U16(6)
    let my_uint: QueryParam    = U32(7)
    let my_ubigint: QueryParam = U64(8)
    let my_string: QueryParam  = "pony"
    let my_blob: QueryParam    = [U8(80); U8(79); U8(78); U8(89)]
    let my_date: QueryParam    = PosixDate(Time.now()._1)
    let params: Array[QueryParam] = [
      my_tiny
      my_small
      my_int
      my_bigint

      my_utiny
      my_usmall
      my_uint
      my_ubigint

      my_string
      my_blob
      my_date
    ]
    DB(h.env, query, params, {(res: Result)(h) =>
      h.assert_eq[U64](1, res.affected_rows())
    })?

class iso _TestOption is UnitTest
  fun name(): String => "mysql/option"

  fun ref apply(h: TestHelper) ? =>
    let mysql = DB.connect(h.env, "mypony")?
    mysql(ConnectTimeout) = I32(42)
    mysql(CharsetName) = "utf8"
    let query = "
      SELECT
        VARIABLE_VALUE
      FROM
        INFORMATION_SCHEMA.SESSION_VARIABLES
      WHERE
        VARIABLE_NAME = ?
        OR VARIABLE_NAME = ?"
    let tmout: QueryParam = "connect_timeout"
    let charset: QueryParam = "character_set_client"
    DB(h.env, query, [tmout; charset], {(res: Result)(h) ? =>
      h.assert_eq[U64](2, res.num_rows())
      match res.fetch_array()?
      | let r: Array[QueryResult] => h.assert_eq[String]("42", r(0)? as String)
      else
        h.assert_true(false, "fetch_array returned None on first row")
      end
      match res.fetch_array()?
      | let r: Array[QueryResult] => h.assert_eq[String]("utf8", r(0)? as String)
      else
        h.assert_true(false, "fetch_array returned None on second row")
      end
    })?

class iso _TestNumResults is UnitTest
  fun name(): String => "mysql/num_results"
  fun exclusion_group(): String => "mysql/with_inserted_data"

  fun ref apply(h: TestHelper) ? =>
    let query = "SELECT * FROM t"
    DB(h.env, query, Array[QueryParam](), {(res: Result)(h) =>
      h.assert_eq[U32](11, res.num_fields())
      h.assert_eq[U64](1, res.num_rows())
    })?

class iso _TestFetchArray is UnitTest
  fun name(): String => "mysql/fetch_array"
  fun exclusion_group(): String => "mysql/with_inserted_data"

  fun ref apply(h: TestHelper) ? =>
    let query = "SELECT * FROM t"
    DB(h.env, query, Array[QueryParam](), {(res: Result)(h) =>
      try
        let err = {(i: USize)(h) =>
          h.assert_true(false, "cast error in column " + i.string())
        }
        match res.fetch_array()?
        | let r: Array[QueryResult] =>
            try h.assert_eq[I8](1,  r(0)? as I8)  else err(0) end
            try h.assert_eq[I16](2, r(1)? as I16) else err(1) end
            try h.assert_eq[I32](3, r(2)? as I32) else err(2) end
            try h.assert_eq[I64](4, r(3)? as I64) else err(3) end
            try h.assert_eq[U8](5,  r(4)? as U8)  else err(4) end
            try h.assert_eq[U16](6, r(5)? as U16) else err(5) end
            try h.assert_eq[U32](7, r(6)? as U32) else err(6) end
            try h.assert_eq[U64](8, r(7)? as U64) else err(7) end
            try h.assert_eq[String]("pony", r(8)? as String) else err(8) end
            try
              let b = r(9)? as Blob
              h.assert_eq[U8](80, b(0)?)
              h.assert_eq[U8](79, b(1)?)
              h.assert_eq[U8](78, b(2)?)
              h.assert_eq[U8](89, b(3)?)
            else
              err(9)
            end
            try
              let d = r(10)? as PosixDate
              let now = PosixDate(Time.now()._1)
              h.assert_eq[I32](now.year, d.year)
              h.assert_eq[I32](now.month, d.month)
              h.assert_eq[I32](now.day_of_month, d.day_of_month)
            else
              err(10)
            end
        | None =>
            h.assert_true(false, "fetch_array returned None on first row")
        end
        h.assert_is[(Array[QueryResult] | None)](None, res.fetch_array()?)
      else
        h.assert_true(false, "fetch failure")
      end
    })?

class iso _TestFetchMap is UnitTest
  fun name(): String => "mysql/fetch_map"
  fun exclusion_group(): String => "mysql/with_inserted_data"

  fun ref apply(h: TestHelper) ? =>
    let query = "SELECT * FROM t"
    DB(h.env, query, Array[QueryParam](), {(res: Result)(h) =>
      try
        let err = {(i: USize)(h) =>
          h.assert_true(false, "cast error in column " + i.string())
        }
        match res.fetch_map()?
        | let m: Map[String, QueryResult] =>
            try h.assert_eq[I8](1,  m("my_tiny")? as I8)  else err(0) end
            try h.assert_eq[I16](2, m("my_small")? as I16) else err(1) end
            try h.assert_eq[I32](3, m("my_int")? as I32) else err(2) end
            try h.assert_eq[I64](4, m("my_bigint")? as I64) else err(3) end
            try h.assert_eq[U8](5,  m("my_utiny")? as U8)  else err(4) end
            try h.assert_eq[U16](6, m("my_usmall")? as U16) else err(5) end
            try h.assert_eq[U32](7, m("my_uint")? as U32) else err(6) end
            try h.assert_eq[U64](8, m("my_ubigint")? as U64) else err(7) end
            try
              h.assert_eq[String]("pony", m("my_string")? as String)
            else
              err(8)
            end
            try
              let b = m("my_blob")? as Blob
              h.assert_eq[U8](80, b(0)?)
              h.assert_eq[U8](79, b(1)?)
              h.assert_eq[U8](78, b(2)?)
              h.assert_eq[U8](89, b(3)?)
            else
              err(9)
            end
            try
              let d = m("my_date")? as PosixDate
              let now = PosixDate(Time.now()._1)
              h.assert_eq[I32](now.year, d.year)
              h.assert_eq[I32](now.month, d.month)
              h.assert_eq[I32](now.day_of_month, d.day_of_month)
            else
              err(10)
            end
        | None =>
            h.assert_true(false, "fetch_array returned None on first row")
        end
        h.assert_is[(Array[QueryResult] | None)](None, res.fetch_array()?)
      else
        h.assert_true(false, "fetch failure")
      end
    })?
