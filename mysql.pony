use "time"
use "debug"

use "lib:mysqlclient"
use "lib:mypony"

primitive _MySQL
primitive _Res
primitive _Field
primitive _Stmt
primitive _Prep
primitive _Bind

//type _Time is (U32, U32, U32, U32, U32, U32, U32, U8, I32)
struct _Time
  var year: U32 = 0
  var month: U32 = 0
  var day: U32 = 0
  var hour: U32 = 0
  var minute: U32 = 0
  var second: U32 = 0
  var second_part: ULong = 0
  var neg: U8 = 0
  var time_type: I32 = 0

primitive _Return
  fun ok(): ISize => 0
  fun no_data(): ISize => 100
  fun data_truncated(): ISize => 101

type QueryParam is
  ( None
  | I8 | I16 | I32 | I64 | ILong | ISize
  | U8 | U16 | U32 | U64 | ULong | USize
  | F32 | F64
  | String
  | PosixDate
  | Array[U8]
  )

type SignedQueryResult is (I8 | I16 | I32 | I64 | ILong | ISize)
type UnsignedQueryResult is (U8 | U16 | U32 | U64 | ULong | USize)
type FloatQueryResult is (F32 | F64)
type Blob is Array[U8] val
type QueryResult is
  ( SignedQueryResult
  | UnsignedQueryResult
  | FloatQueryResult
  | String
  | Blob
  | PosixDate
  | None
  )

// Do this here as mysql_server_init() is not thread-safe. This avoids
// the need for a global mutex around mysql_init().
primitive _Init
  fun _init() =>
    let c: I32 = 0
    @mysql_server_init[None](c, Pointer[Pointer[U8]], Pointer[Pointer[U8]])

class MySQL
  var _mysql: Pointer[_MySQL]
  let _notify: Notify

  new create(notify: Notify) =>
    _mysql  = Pointer[_MySQL]
    _notify = notify

  fun ref connect(host: (String | None) = None,
                  user: (String | None) = None,
                  pass: (String | None) = None,
                  db:   (String | None) = None,
                  port: (U16 | None)    = 0,
                  sock: (String | None) = None): MySQL ? =>
    let host' = match host | let s: String => s.cstring() else Pointer[U8] end
    let user' = match user | let s: String => s.cstring() else Pointer[U8] end
    let pass' = match pass | let s: String => s.cstring() else Pointer[U8] end
    let db'   = match db   | let s: String => s.cstring() else Pointer[U8] end
    let port' = match port | let p: U16    => p           else 0           end
    let sock' = match sock | let s: String => s.cstring() else Pointer[U8] end
    let flags: USize = 0
    _mysql_init()?
    let r = @mysql_real_connect[Pointer[_MySQL]](
      _mysql, host', user', pass', db', port', sock', flags
    )
    if r.is_null() then
      _notify.fail(Error("connect", error_message(), errno()))
      error
    end
    this

  fun ref tcp(host: String,
              user: (String | None) = None,
              pass: (String | None) = None,
              db:   (String | None) = None,
              port: (U16 | None)    = None): MySQL ? =>
    connect(host, user, pass, db, port, None)?

  fun ref unix(sock: String,
               user: (String | None) = None,
               pass: (String | None) = None,
               db:   (String | None) = None): MySQL ? =>
    connect(None, user, pass, db, None, sock)?

  fun ping(): Bool =>
    @mysql_ping[I32](_mysql) == 0

  fun ref prepare(query: String): Stmt ? =>
    let stmt = @mypony_stmt_init[Pointer[_Stmt]](_mysql)
    if stmt.is_null() then
      _notify.fail(Error("prepare", error_message(), errno()))
      error
    end
    if @mysql_stmt_prepare[I32](stmt, query.cstring(), query.size()) != 0 then
      _notify.fail(Error("prepare", error_message(), errno()))
      error
    end
    Stmt._create(stmt, _notify)

  fun ref update(opt: ClientOption, value: Stringable) =>
    var s = value.string()
    @mysql_options[None](_mysql, opt(), addressof s)

  fun errno(): U32 =>
    @mysql_errno[U32](_mysql)

  fun error_message(): String =>
    Util.copy_cpointer(@mysql_error[Pointer[U8] iso^](_mysql))

  fun ref _mysql_init() ? =>
    _mysql = @mysql_init[Pointer[_MySQL]](_mysql)
    if _mysql.is_null() then
      _notify.fail(Error("init", error_message(), errno()))
      error
    end

  fun dispose() =>
    @mysql_close[None](_mysql)

  fun library_end() =>
    @mysql_server_end[None]()
