#include <mysql/mysql.h>
#include <pony.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>

struct mypony_bind {
    unsigned long  n;
    MYSQL_BIND    *bind;
    unsigned long *length;
    my_bool       *is_null;
    my_bool        is_unsigned;
};

struct mypony_bind *
mypony_alloc_bind(unsigned long count)
{
    pony_ctx_t *ctx = pony_ctx();
    struct mypony_bind *mbp = pony_alloc(ctx, sizeof *mbp);

    if (mbp == NULL)
        return NULL;

    mbp->n       = count;
    mbp->bind    = pony_alloc(ctx, count * sizeof(MYSQL_BIND));
    mbp->length  = pony_alloc(ctx, count * sizeof(unsigned long));
    mbp->is_null = pony_alloc(ctx, count * sizeof(my_bool));

    return mbp;
}

unsigned long
mypony_bind_count(struct mypony_bind *mbp)
{
    return mbp->n;
}

unsigned long
mypony_bind_is_null(struct mypony_bind *mbp, unsigned long i)
{
    return mbp->is_null[i];
}

my_bool
mypony_bind_is_unsigned(struct mypony_bind *mbp, unsigned long i)
{
    return mbp->bind[i].is_unsigned;
}

unsigned long
mypony_bind_buffer_type(struct mypony_bind *mbp, unsigned long i)
{
    return mbp->bind[i].buffer_type;
}

MYSQL_STMT *
mypony_stmt_init(MYSQL *mysql)
{
    my_bool t = 1;
    MYSQL_STMT *stmt = mysql_stmt_init(mysql);
    mysql_stmt_attr_set(stmt, STMT_ATTR_UPDATE_MAX_LENGTH, &t);
    return stmt;
}

my_bool
mypony_stmt_bind_param(MYSQL_STMT *stmt, struct mypony_bind *mbp)
{
    return mysql_stmt_bind_param(stmt, mbp->bind);
}

struct mypony_bind *
mypony_alloc_result(MYSQL_STMT *stmt, MYSQL_RES *res)
{
    unsigned long i, n;
    struct mypony_bind *result;

    if (res == NULL)
        return NULL;

    n = mysql_num_fields(res);
    result = mypony_alloc_bind(n);
    for (i = 0; i < n; i++) {
        MYSQL_BIND *bind = &result->bind[i];
        MYSQL_FIELD *field = mysql_fetch_field_direct(res, i);
        bind->buffer_type = field->type;
        bind->length      = &result->length[i];
        bind->is_null     = &result->is_null[i];
        bind->is_unsigned = ((field->flags & UNSIGNED_FLAG) != 0);
    }
    return result;
}

int
mypony_bind_result(MYSQL_STMT *stmt, MYSQL_RES *res, struct mypony_bind *result)
{
    unsigned long i;
    pony_ctx_t *ctx;

    if (res == NULL)
        return 0;
    if (mysql_stmt_store_result(stmt))
        return -1;

    ctx = pony_ctx();
    for (i = 0; i < result->n; i++) {
        MYSQL_BIND *bind = &result->bind[i];
        MYSQL_FIELD *field = mysql_fetch_field_direct(res, i);
        /*
         * sizes from
         * https://dev.mysql.com/doc/refman/5.6/en/mysql-stmt-fetch.html
         */
        switch (bind->buffer_type) {
        case MYSQL_TYPE_NULL:
            break;
        case MYSQL_TYPE_TINY:
        case MYSQL_TYPE_YEAR:
            bind->buffer_length = 1;
            bind->buffer = pony_alloc(ctx, bind->buffer_length);
            break;
        case MYSQL_TYPE_SHORT:
            bind->buffer_length = 2;
            bind->buffer = pony_alloc(ctx, bind->buffer_length);
        case MYSQL_TYPE_INT24:
        case MYSQL_TYPE_LONG:
        case MYSQL_TYPE_FLOAT:
            bind->buffer_length = 4;
            bind->buffer = pony_alloc(ctx, bind->buffer_length);
            break;
        case MYSQL_TYPE_LONGLONG:
        case MYSQL_TYPE_DOUBLE:
            bind->buffer_length = 8;
            bind->buffer = pony_alloc(ctx, bind->buffer_length);
            break;
        case MYSQL_TYPE_DECIMAL:
        case MYSQL_TYPE_STRING:
        case MYSQL_TYPE_VAR_STRING:
        case MYSQL_TYPE_TINY_BLOB:
        case MYSQL_TYPE_BLOB:
        case MYSQL_TYPE_MEDIUM_BLOB:
        case MYSQL_TYPE_LONG_BLOB:
        case MYSQL_TYPE_NEWDECIMAL:
        case MYSQL_TYPE_BIT:
            bind->buffer_length = field->max_length * sizeof(char);
            bind->buffer = pony_alloc(ctx, bind->buffer_length);
            break;
        case MYSQL_TYPE_TIME:
        case MYSQL_TYPE_DATE:
        case MYSQL_TYPE_DATETIME:
        case MYSQL_TYPE_TIMESTAMP:
            bind->buffer_length = sizeof(MYSQL_TIME);
            bind->buffer = pony_alloc(ctx, bind->buffer_length);
            break;
        default:
            return -1;
        }
    }
    return mysql_stmt_bind_result(stmt, result->bind);
}

static void
mypony_bind_param(struct mypony_bind *params, enum enum_field_types type,
                  void *data, unsigned long len, my_bool is_unsigned,
                  unsigned long i)
{
    MYSQL_BIND *bind = &params->bind[i];

    params->length[i] = len;
    bind->length = &params->length[i];
    bind->is_unsigned = is_unsigned;
    bind->buffer_type = type;
    bind->buffer_length = len;
    bind->buffer = malloc(len);
    memcpy(bind->buffer, data, len);
}

void
mypony_null_param(struct mypony_bind *params, unsigned long i)
{
    mypony_bind_param(params, MYSQL_TYPE_NULL, NULL, 0, 0, i);
}

void
mypony_time_param(struct mypony_bind *params,
                  unsigned int year, unsigned int month, unsigned int day,
                  unsigned int hour, unsigned int minute, unsigned int second,
                  unsigned long i)
{
    MYSQL_TIME t;
    size_t siz = sizeof(t);

    memset(&t, 0, siz);

    t.year   = year;
    t.month  = month;
    t.day    = day;
    t.hour   = hour;
    t.minute = minute;
    t.second = second;

    mypony_bind_param(params, MYSQL_TYPE_DATETIME, &t, siz, 0, i);
}

#define STRING_PARAM(name, c)                              \
void                                                       \
mypony_##name##_param(struct mypony_bind *params, char *s, \
                      unsigned long len, unsigned long i)  \
{                                                          \
    mypony_bind_param(params, c, s, len, 0, i);            \
}

#define SIGNED_PARAM(name, type, c)                                        \
void                                                                       \
mypony_##name##_param(struct mypony_bind *params, type p, unsigned long i) \
{                                                                          \
    mypony_bind_param(params, c, &p, sizeof(p), 0, i);                     \
}

#define UNSIGNED_PARAM(name, type, c)                                       \
void                                                                        \
mypony_u##name##_param(struct mypony_bind *params, type p, unsigned long i) \
{                                                                           \
    mypony_bind_param(params, c, &p, sizeof(p), 1, i);                      \
}

STRING_PARAM(blob,   MYSQL_TYPE_BLOB)
STRING_PARAM(string, MYSQL_TYPE_STRING)

SIGNED_PARAM(tiny,     int8_t,  MYSQL_TYPE_TINY)
SIGNED_PARAM(short,    int16_t, MYSQL_TYPE_SHORT)
SIGNED_PARAM(long,     int32_t, MYSQL_TYPE_LONG)
SIGNED_PARAM(longlong, int64_t, MYSQL_TYPE_LONGLONG)
SIGNED_PARAM(float,    float,   MYSQL_TYPE_FLOAT)
SIGNED_PARAM(double,   double,  MYSQL_TYPE_DOUBLE)

UNSIGNED_PARAM(tiny,     uint8_t,  MYSQL_TYPE_TINY)
UNSIGNED_PARAM(short,    uint16_t, MYSQL_TYPE_SHORT)
UNSIGNED_PARAM(long,     uint32_t, MYSQL_TYPE_LONG)
UNSIGNED_PARAM(longlong, uint64_t, MYSQL_TYPE_LONGLONG)

#define RESULT(name, type)                                          \
type                                                                \
mypony_##name##_result(struct mypony_bind *result, unsigned long i) \
{                                                                   \
    return *(type *)result->bind[i].buffer;                         \
}

RESULT(u8,  uint8_t)
RESULT(u16, uint16_t)
RESULT(u32, uint32_t)
RESULT(u64, uint64_t)

RESULT(i8,  int8_t)
RESULT(i16, int16_t)
RESULT(i32, int32_t)
RESULT(i64, int64_t)
RESULT(f32, float)
RESULT(f64, double)

char *
mypony_string_result(struct mypony_bind *result, unsigned long *len,
                     unsigned long i)
{
    *len = result->length[i];
    return result->bind[i].buffer;
}

void
mypony_time_result(struct mypony_bind *result, unsigned long i, MYSQL_TIME *t)
{
    memcpy(t, result->bind[i].buffer, sizeof(MYSQL_TIME));
}

const char *
mypony_field_name(MYSQL_FIELD *field)
{
    return field->name;
}

int
mypony_field_type(MYSQL_FIELD *field)
{
    return field->type;
}

const char *
mypony_string_at(char **a, unsigned int i)
{
    return a[i];
}
