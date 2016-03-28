use "collections"
use "time"
use "debug"

primitive _T
  fun decimal(): USize     => 0
  fun tiny(): USize        => 1
  fun short(): USize       => 2
  fun long(): USize        => 3
  fun float(): USize       => 4
  fun double(): USize      => 5
  fun null(): USize        => 6
  fun timestamp(): USize   => 7
  fun long_long(): USize   => 8
  fun int24(): USize       => 9
  fun date(): USize        => 10
  fun time(): USize        => 11
  fun datetime(): USize    => 12
  fun year(): USize        => 13
  fun new_date(): USize    => 14
  fun var_char(): USize    => 15
  fun bit(): USize         => 16
  fun new_decimal(): USize => 246
  fun tiny_blob(): USize   => 249
  fun medium_blob(): USize => 250
  fun long_blob(): USize   => 251
  fun blob(): USize        => 252
  fun var_string(): USize  => 253
  fun string(): USize      => 254

class Result
  let _stmt: Pointer[_Stmt]
  let _notify: Notify
  let _params: Pointer[_Bind]
  let _result: Pointer[_Bind]
  let _res: Pointer[_Res]

  new _create(stmt: Pointer[_Stmt], notify: Notify,
              params: Pointer[_Bind], result: Pointer[_Bind],
              res: Pointer[_Res]) =>
    _stmt   = stmt
    _notify = notify
    _params = params
    _result = result
    _res    = res

  fun affected_rows(): U64 =>
    @mysql_stmt_affected_rows[U64](_stmt)

  fun num_rows(): U64 =>
    @mysql_stmt_num_rows[U64](_stmt)

  fun num_fields(): U32 =>
    @mysql_num_fields[U32](_res)

  fun ref map_rows(): ResultMapIter =>
    ResultMapIter._create(this, _notify)

  fun ref fetch_map(): (Map[String, QueryResult] | None) ? =>
    let that = this
    let f = lambda(n: USize)(that): Map[String, QueryResult] ? =>
      that._map_row(n)
    end
    _fetch_with[Map[String, QueryResult]](f)

  fun ref array_rows(): ResultArrayIter =>
    ResultArrayIter._create(this, _notify)

  fun ref fetch_array(): (Array[QueryResult] | None) ? =>
    let that = this
    let f = lambda(n: USize)(that): Array[QueryResult] ? =>
      that._array_row(n)
    end
    _fetch_with[Array[QueryResult]](f)

  fun _fetch_with[T](f: {(USize): T ?}): (T | None) ? =>
    match @mysql_stmt_fetch[ISize](_stmt)
    | _Return.no_data() => None
    | _Return.ok()      => f(@mypony_bind_count[USize](_result))
    | _Return.data_truncated() =>
        _notify.fail(Error("fetch", "data truncated"))
        error
    else
      _notify.fail(Error("fetch", error_message(), errno()))
      error
    end

  fun _map_row(n: USize): Map[String, QueryResult] ? =>
    let row = Map[String, QueryResult]
    for i in Range(0, n) do
      let field = @mysql_fetch_field_direct[Pointer[_Field]](_res, i)
      let name = Util.copy_cstring(@mypony_field_name[Pointer[U8] val](field))
      if @mypony_bind_is_null[Bool](_result, i) then
        row(name) = None
      else
        row(name) = _convert_result(i)
      end
    end
    row

  fun _array_row(n: USize): Array[QueryResult] ? =>
    let row = Array[QueryResult](n)
    for i in Range(0, n) do
      if @mypony_bind_is_null[Bool](_result, i) then
        row.push(None)
      else
        row.push(_convert_result(i))
      end
    end
    row

  fun _convert_result(i: USize): QueryResult ? =>
    let t = @mypony_bind_buffer_type[USize](_result, i)
    if @mypony_bind_is_unsigned[Bool](_result, i) then
      _convert_unsigned(t, i)
    else
      _convert(t, i)
    end

  fun _convert_unsigned(t: USize, i: USize): (UnsignedQueryResult | Date) ? =>
    match t
    | _T.tiny() | _T.year()  => @mypony_u8_result[U8](_result, i)
    | _T.short()             => @mypony_u16_result[U16](_result, i)
    | _T.int24() | _T.long() => @mypony_u32_result[U32](_result, i)
    | _T.long()              => @mypony_u32_result[U32](_result, i)
    | _T.long_long()         => @mypony_u64_result[U64](_result, i)
    | _T.timestamp()         => _date(@mypony_time_result[_Time](_result, i))
    else
      _notify.fail(Error("fetch", "invalid unsigned type: " + t.string()))
      error
    end

  fun _convert(t: USize, i: USize):
      (SignedQueryResult | FloatQueryResult | String | Blob | Date) ? =>
    match t
    | _T.tiny() | _T.year() =>
        @mypony_i8_result[I8](_result, i)
    | _T.short() =>
        @mypony_i16_result[I16](_result, i)
    | _T.int24() | _T.long() =>
        @mypony_i32_result[I32](_result, i)
    | _T.long() =>
        @mypony_i32_result[I32](_result, i)
    | _T.long_long() =>
        @mypony_i64_result[I64](_result, i)
    | _T.float() =>
        @mypony_f32_result[F32](_result, i)
    | _T.double() =>
        @mypony_f64_result[F64](_result, i)
    | _T.date() | _T.time() | _T.datetime() | _T.timestamp() =>
        _date(@mypony_time_result[_Time](_result, i))
    | _T.decimal() | _T.new_decimal() | _T.string() | _T.var_string()
    | _T.bit() =>
        var len: USize = 0
        let cs =
          @mypony_string_result[Pointer[U8] val](_result, addressof len, i)
        Util.copy_cstring(cs, len)
    | _T.tiny_blob() | _T.medium_blob() | _T.long_blob() | _T.blob() =>
        var len: USize = 0
        let cs =
          @mypony_string_result[Pointer[U8] val](_result, addressof len, i)
        Util.copy_cstring(cs, len).array()
    else
      _notify.fail(Error("fetch", "unknown type " + t.string()))
      error
    end

  fun _date(tm: _Time): Date =>
    let date = Date()
    date.year  = tm._1.i32()
    date.month = tm._2.i32()
    date.day_of_month = tm._3.i32()
    date.hour  = tm._4.i32()
    date.min   = tm._5.i32()
    date.sec   = tm._6.i32()
    date

  fun errno(): U32 =>
    @mysql_stmt_errno[U32](_stmt)

  fun error_message(): String =>
    Util.copy_cstring(@mysql_stmt_error[Pointer[U8] val](_stmt))

  fun close() =>
    @mypony_bind_buffers_free[None](_params)
    @mypony_bind_buffers_free[None](_result)

  fun dispose() =>
    close()

class ResultMapIter
  let _result: Result
  let _notify: Notify
  var _cur: U64
  let _max: U64

  new _create(result: Result, notify: Notify) =>
    _result = result
    _notify = notify
    _cur    = 0
    _max    = result.num_rows()

  fun has_next(): Bool =>
    _cur < _max

  fun ref next(): Map[String, QueryResult] ? =>
    _cur = _cur + 1
    let row = _result.fetch_map()
    match row
    | let m: Map[String, QueryResult] => m
    else
      _notify.fail(Error("next", "fetch_map returned None in iterator"))
      error
    end

  fun dispose() =>
    _result.close()

class ResultArrayIter
  let _result: Result
  let _notify: Notify
  var _cur: U64
  let _max: U64

  new _create(result: Result, notify: Notify) =>
    _result = result
    _notify = notify
    _cur    = 0
    _max    = result.num_rows()

  fun has_next(): Bool =>
    _cur < _max

  fun ref next(): Array[QueryResult] ? =>
    _cur = _cur + 1
    let row = _result.fetch_array()
    match row
    | let r: Array[QueryResult] => r
    else
      _notify.fail(Error("next", "fetch_array returned None in iterator"))
      error
    end

  fun dispose() =>
    _result.close()
