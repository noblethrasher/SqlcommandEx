using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Data;
using System.Dynamic;
using System.Linq.Expressions;

namespace Prelude
{
    public class DynamicDataReader<T> : IDataReader, IEnumerable<DynamicDataReader<T>>, IDynamicMetaObjectProvider where T : IDataReader
    {
        T reader;


        public DynamicDataReader(T reader)
        {
            this.reader = reader;
        }

        public void Close()
        {
            reader.Close ();
        }

        public int Depth
        {
            get { return reader.Depth; }
        }

        public DataTable GetSchemaTable()
        {
            return reader.GetSchemaTable ();
        }

        public bool IsClosed
        {
            get { return reader.IsClosed; }
        }

        public bool NextResult()
        {
            return reader.NextResult ();
        }

        public bool Read()
        {
            return reader.Read ();
        }

        public int RecordsAffected
        {
            get { return reader.RecordsAffected; }
        }

        public void Dispose()
        {
            reader.Dispose ();
        }

        public int FieldCount
        {
            get { return reader.FieldCount; }
        }

        public bool GetBoolean(int i)
        {
            return reader.GetBoolean (i);
        }

        public byte GetByte(int i)
        {
            return reader.GetByte (i);
        }

        public long GetBytes(int i, long fieldOffset, byte[] buffer, int bufferoffset, int length)
        {
            return reader.GetBytes (i, fieldOffset, buffer, bufferoffset, length);
        }

        public char GetChar(int i)
        {
            return reader.GetChar (i);
        }

        public long GetChars(int i, long fieldoffset, char[] buffer, int bufferoffset, int length)
        {
            return reader.GetChars (i, fieldoffset, buffer, bufferoffset, length);
        }

        public IDataReader GetData(int i)
        {
            return reader.GetData (i);
        }

        public string GetDataTypeName(int i)
        {
            return reader.GetDataTypeName (i);
        }

        public DateTime GetDateTime(int i)
        {
            return reader.GetDateTime (i);
        }

        public decimal GetDecimal(int i)
        {
            return reader.GetDecimal (i);
        }

        public double GetDouble(int i)
        {
            return reader.GetDouble (i);
        }

        public Type GetFieldType(int i)
        {
            return reader.GetFieldType (i);
        }

        public float GetFloat(int i)
        {
            return reader.GetFloat (i);
        }

        public Guid GetGuid(int i)
        {
            return reader.GetGuid (i);
        }

        public short GetInt16(int i)
        {
            return reader.GetInt16 (i);
        }

        public int GetInt32(int i)
        {
            return reader.GetInt32 (i);
        }

        public long GetInt64(int i)
        {
            return reader.GetInt64 (i);
        }

        public string GetName(int i)
        {
            return reader.GetName (i);
        }

        public int GetOrdinal(string name)
        {
            return reader.GetOrdinal (name);
        }

        public string GetString(int i)
        {
            return reader.GetString (i);
        }

        public object GetValue(int i)
        {
            return reader.GetValue (i);
        }

        public int GetValues(object[] values)
        {
            return reader.GetValues (values);
        }

        public bool IsDBNull(int i)
        {
            return reader.IsDBNull (i);
        }

        public object this[string name]
        {
            get { return reader[name]; }
        }

        public object this[int i]
        {
            get { return reader[i]; }
        }

        DynamicMetaObject IDynamicMetaObjectProvider.GetMetaObject(System.Linq.Expressions.Expression parameter)
        {
            return new _dynamicReaderMetaObject (reader, parameter);
        }

        class _dynamicReaderMetaObject : DynamicMetaObject
        {
            T reader;

            public _dynamicReaderMetaObject(T reader, Expression exp, BindingRestrictions restrictions = null)
                : base (exp, restrictions ?? BindingRestrictions.Empty, reader)
            {
                this.reader = reader;
            }

            public override DynamicMetaObject BindGetMember(GetMemberBinder binder)
            {
                Expression<Func<object>> f = () => reader[binder.Name];

                var exp = Expression.Invoke (f, null);
                var exp1 = Expression.Convert (exp, binder.ReturnType);

                return new DynamicMetaObject (exp1, BindingRestrictions.GetInstanceRestriction (Expression.Constant (reader), reader));
            }


            public override DynamicMetaObject BindConvert(ConvertBinder binder)
            {
                if (binder.ReturnType == typeof (System.Collections.IEnumerable))
                {
                    var exp = Expression.New (typeof (DynamicDataReader<T>).GetConstructor (new[] { typeof (T) }), new[] { Expression.Constant (reader) });

                    var exp1 = Expression.Convert (exp, typeof (System.Collections.IEnumerable));

                    return new DynamicMetaObject (exp1, BindingRestrictions.GetInstanceRestriction (Expression.Constant (reader), reader));
                }

                throw new InvalidCastException ("Cannot cast to " + binder.ReturnType.Name);
                
            }

            public override DynamicMetaObject BindInvokeMember(InvokeMemberBinder binder, DynamicMetaObject[] args)
            {
                if (binder.ReturnType == typeof (System.Collections.IEnumerable))
                {

                    //var exp = Expression.New (typeof (_dynamicEnumerable).GetConstructor (new[] { typeof (T) }), new[] { Expression.Constant (reader) });

                    return new _dynamicReaderMetaObject (reader, Expression, BindingRestrictions.GetInstanceRestriction (Expression.Constant (reader), reader));

                }

                throw new System.ArgumentException ("There is no method " + binder.Name);
            }            

        }

        public IEnumerator<DynamicDataReader<T>> GetEnumerator()
        {
            while (reader.Read ())
            {
                yield return this;
            }
        }

        System.Collections.IEnumerator System.Collections.IEnumerable.GetEnumerator()
        {
            return GetEnumerator ();
        }
    }
}
