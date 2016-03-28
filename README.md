# Pony-MySQL

These are [Pony](http://www.ponylang.org/) bindings for [libmysqlclient](http://dev.mysql.com/doc/refman/5.7/en/c-api.html).

The bindings only support the prepared statements API. Usage is as follows:

```pony
use "mysql"

class MyNotify is Notify
  let _env: Env

  new create(env: Env) =>
    _env = env

  fun fail(err: Error) =>
    _env.out.print(err.method() + " failed: " + err.string())

actor Main
  new create(env: Env) =>
    try
      let mysql = MySQL(MyNotify(env)).tcp("host", "user", "pass", "db")
      mysql(CharsetName) = "utf8"
      let query = "SELECT * FROM some_table where some_field = ?"
      let some_field: QueryParam = "some_value"
      with stmt = mysql.prepare(query) do
        with res = stmt.execute([some_field]) do
          match res.fetch_map()
          | let row: Map[String, QueryResult] => _do_something(row)
          | None => env.out.print("no more results")
          end
        end
      end
    end

  fun _do_something(m: Map[String, QueryResult]) =>
    ...
```

If you don't want to use `with` expressions you must call `close()` on `Stmt`
and `Result` objects to free their resources.

To run the unit tests, export environment variables `MYPONY_HOST`, `MYPONY_USER`
and `MYPONY_PASS` with the appropriate information.
