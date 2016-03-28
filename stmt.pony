use "time"

class Stmt
  let _stmt: Pointer[_Stmt]
  let _notify: Notify
  let _params: Pointer[_Bind]
  let _result: Pointer[_Bind]
  let _res: Pointer[_Res]
  let _num_params: USize

  new _create(stmt: Pointer[_Stmt], notify: Notify) =>
    _stmt       = stmt
    _notify     = notify
    _num_params = @mysql_stmt_param_count[USize](_stmt)
    _params     = @mypony_alloc_bind[Pointer[_Bind]](_num_params)
    _res        = @mysql_stmt_result_metadata[Pointer[_Res]](_stmt)
    _result     = @mypony_alloc_result[Pointer[_Bind]](_stmt, _res)

  fun ref execute(args: Array[QueryParam] = Array[QueryParam]()): Result ? =>
    let num_args = args.size()
    if num_args != _num_params then
      let err = "argument count (" + num_args.string() + ") "
              + "!= param count (" + _num_params.string() + ")"
      _notify.fail(Error("execute", err))
      error
    end
    if num_args > 0 then
      _bind_params(args)
    end
    if @mysql_stmt_execute[ISize](_stmt) != 0 then
      _execute_error()
    end
    if @mypony_bind_result[ISize](_stmt, _res, _result) != 0 then
      _execute_error()
    end
    Result._create(_stmt, _notify, _params, _result, _res)

  fun reset() ? =>
    if @mysql_stmt_reset[U8](_stmt) != 0 then
      error
    end

  fun errno(): U32 =>
    @mysql_stmt_errno[U32](_stmt)

  fun error_message(): String =>
    Util.copy_cstring(@mysql_stmt_error[Pointer[U8] val](_stmt))

  fun _bind_params(args: Array[QueryParam]) ? =>
    for (i, arg) in args.pairs() do
      match arg
      | None =>
          @mypony_null_param[None](_params, i)
      | let x: I8 =>
          @mypony_tiny_param[None](_params, x, i)
      | let x: I16 =>
          @mypony_short_param[None](_params, x, i)
      | let x: I32 =>
          @mypony_long_param[None](_params, x, i)
      | let x: (I64 | ILong | ISize) =>
          @mypony_longlong_param[None](_params, x.i64(), i)
      | let x: U8 =>
          @mypony_utiny_param[None](_params, x, i)
      | let x: U16 =>
          @mypony_ushort_param[None](_params, x, i)
      | let x: U32 =>
          @mypony_ulong_param[None](_params, x, i)
      | let x: (U64 | ULong | USize) =>
          @mypony_ulonglong_param[None](_params, x.u64(), i)
      | let x: F32 =>
          @mypony_float_param[None](_params, x, i)
      | let x: F64 =>
          @mypony_double_param[None](_params, x, i)
      | let x: String =>
          @mypony_string_param[None](_params, x.cstring(), x.size(), i)
      | let d: Date =>
          @mypony_time_param[None](
            _params, d.year.u32(), d.month.u32(), d.day_of_month.u32(),
            d.hour.u32(), d.min.u32(), d.sec.u32(), i
          )
      | let b: Array[U8] =>
          @mypony_blob_param[None](_params, b.cstring(), b.size(), i)
      else
        _notify.fail(Error("execute", "unsupported parameter type", 2036))
        error
      end
    end
    if @mypony_stmt_bind_param[I8](_stmt, _params) != 0 then
      _execute_error()
    end

  fun _execute_error() ? =>
    _notify.fail(Error("execute", error_message(), errno()))
    error

  fun close() =>
    @mypony_bind_free[None](_params)
    @mypony_bind_free[None](_result)
    @mysql_free_result[None](_res)
    @mysql_stmt_close[None](_stmt)

  fun dispose() =>
    close()
