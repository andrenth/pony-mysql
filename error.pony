class Error
  let _meth: String
  let _msg: String
  let _num: U32

  new create(meth: String, msg: String, num: U32 = 0) =>
    _meth = meth
    _msg = msg
    _num = num

  fun method(): String => _meth
  fun message(): String => _msg
  fun number(): (U32 | None) => if _num == 0 then None else _num end

  fun string(): String =>
    match _num
    | 0 => _meth + ": " + _msg
    else
      _meth + ": " + _num.string() + " " + _msg
    end
