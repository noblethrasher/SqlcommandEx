using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Data.SqlClient;
using System.Configuration;
using System.Data;
using System.Dynamic;
using System.Linq.Expressions;


namespace Chapman.DataUtils
{
    public class ColumnMappingAttribute : Attribute
    {
        public string ColumnName { get; private set; }
        public object DefaultValue { get; private set; }

        public ColumnMappingAttribute(string Name) : this(Name, null)
        {

        }

        public ColumnMappingAttribute(string Name, object DefaultValue)
        {
            this.ColumnName = Name;
            this.DefaultValue = DefaultValue;
        }
    }


    public interface IDataInstantiable
    {
        void Hydrate(IDataReader reader);
    }


    /// <summary>
    /// Looks for a connection called "default_readonly"
    /// </summary>
    public sealed class SQLRO : SQL
    {
        public SQLRO(string command, string connection, SqlParameter[] sqlparams) : base (command, connection, sqlparams) { }

        protected override string GetConnectionString()
        {
            return ConfigurationManager.ConnectionStrings["default_readonly"].ConnectionString;
        }
    }

    /// <summary>
    /// Looks for a connection string "default_readwrite"
    /// </summary>
    public sealed class SQLRW : SQL
    {
        public SQLRW(string command, string connection, SqlParameter[] sqlparams) : base(command, connection, sqlparams) { }

        protected override string GetConnectionString()
        {
            return ConfigurationManager.ConnectionStrings["default_readwrite"].ConnectionString;
        }
    }


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
            return reader.GetOrdinal(name);
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

            public _dynamicReaderMetaObject(T reader, Expression exp, BindingRestrictions restrictions = null) : base(exp, restrictions ?? BindingRestrictions.Empty, reader)
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

                throw new Exception ("!!!");
            }

            public override DynamicMetaObject BindInvokeMember(InvokeMemberBinder binder, DynamicMetaObject[] args)
            {
                if (binder.ReturnType == typeof (System.Collections.IEnumerable))
                {

                    //var exp = Expression.New (typeof (_dynamicEnumerable).GetConstructor (new[] { typeof (T) }), new[] { Expression.Constant (reader) });

                    return new _dynamicReaderMetaObject (reader, Expression, BindingRestrictions.GetInstanceRestriction (Expression.Constant (reader), reader));

                }

                throw new Exception ("!!!");
            }

            //class _dynamicEnumerable : IEnumerable<DynamicDataReader<T>>
            //{
            //    T reader;

            //    public _dynamicEnumerable(T reader)
            //    {
            //        this.reader = reader;
            //    }


            //    public IEnumerator<DynamicDataReader<T>> GetEnumerator()
            //    {
            //        while (reader.Read ())
            //        {
            //            yield return new DynamicDataReader<T> (reader);
            //        }

            //        reader.Dispose ();
            //    }

            //    System.Collections.IEnumerator System.Collections.IEnumerable.GetEnumerator()
            //    {
            //        return GetEnumerator ();
            //    }
            //}


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

    public class SQL : IEnumerable<DynamicDataReader<SqlDataReader>>, IDisposable
    {

        string sql;
        public string CommandText
        {
            get
            {
                return sql;
            }
            set
            {
                sql = value;
            }
        }


        string connection;

        List<SqlParameter> _params;

        Action dispose_acts;
        public event Action Dispose
        {
            add
            {
                dispose_acts += value;
            }
            remove
            {

            }
        }

        protected virtual string GetConnectionString()
        {
            return ConfigurationManager.ConnectionStrings["default"].ConnectionString;
        }

        public void Add(IDictionary<string, object> dict)
        {
            var xs = dict.Select (x => new SqlParameter (x.Key, x.Value));

            (_params = _params ?? new List<SqlParameter> ()).AddRange (xs);
        }

        public void Add(string paramName, object value)
        {
            (_params = _params ?? new List<SqlParameter> ()).Add (new SqlParameter (paramName, value));
        }

        public void Add(SqlParameter prm)
        {
            (_params = _params ?? new List<SqlParameter> ()).Add (prm);
        }

        public void AddRange(IEnumerable<SqlParameter> prms)
        {
            (_params = _params ?? new List<SqlParameter> ()).AddRange (prms);
        }


        /// <summary>
        /// Creates a parameter for each property in T with name = @PropertyName then adds the parameter collection to an internal array of parameters
        /// </summary>
        /// <typeparam name="T"></typeparam>
        public void AddParametersFromType<T>()
        {
            var explicitly_mapped =
                        from p in typeof (T).GetProperties ()
                        let c = (ColumnMappingAttribute[])p.GetCustomAttributes (typeof (ColumnMappingAttribute), false)
                        where c.Length > 0 && p.CanRead
                        let name = c[0].ColumnName
                        select new { param = new SqlParameter ("@" + name, null), property = p };


            var implicitly_mapped =
                       from p in typeof (T).GetProperties ()
                       where !explicitly_mapped.Select(x => x.property).Contains(p) && p.CanRead
                       select new { param = new SqlParameter ("@" + p.Name, null), property = p };

            _params = _params ?? new List<SqlParameter> ();
            _params.Clear ();

            _params.AddRange (explicitly_mapped.Union (implicitly_mapped).Select (x => x.param));



        }

        public SQL(string SqlString, string Connection = null, params SqlParameter[] SqlParams)
        {
            this.sql = SqlString;
            this.connection = Connection ?? GetConnectionString ();
        }

        public static implicit operator SQL(string s)
        {
            return new SQL (s);
        }

        public static implicit operator SqlParameter[](SQL _sql)
        {
            return _sql._params.ToArray ();
        }

        public IEnumerator<DynamicDataReader<SqlDataReader>> GetEnumerator()
        {
            using (var conn = new SqlConnection (connection))
            {
                conn.Open ();

                var command = new SqlCommand (sql, conn);
                var reader = command.ExecuteReader ();

                while (reader.Read ())
                    yield return new DynamicDataReader<SqlDataReader>(reader);

                conn.Close ();
            }
        }

        public SqlCommand GetSqlCommand(SqlConnection sc = null)
        {
            var conn_act = CreateConnectionDisposeAction (sc);

            Dispose += conn_act.act;

            var command = new SqlCommand (sql, conn_act.conn);
            command.Parameters.AddRange (_params.ToArray ());



            if (sql.StartsWith ("[") || sql.EndsWith ("]") || !sql.All (c => char.IsWhiteSpace (c)))
                command.CommandType = CommandType.StoredProcedure;
            else
                command.CommandType = CommandType.Text;
            

            return command;
        }

        public SqlDataReader GetSqlDataReader(SqlConnection sc = null)
        {
            var rdr = GetSqlCommand (sc).ExecuteReader ();

            Dispose += () => { rdr.Dispose (); };

            return rdr;
        }

        public DataSet GetDataSet(SqlConnection sc = null)
        {
            var conn_act = CreateConnectionDisposeAction (sc);

            Dispose += conn_act.act;

            var ds = new DataSet ();
            var da = new SqlDataAdapter(new SqlCommand(sql, conn_act.conn));

            da.Fill(ds);

            return ds;
        }

        struct ActionConnection
        {
            public Action act;
            public SqlConnection conn;
        }

        /// <summary>
        /// Creates a dispose delegate that will be called when this object is disposed.
        /// </summary>
        /// <param name="sql_conn"></param>
        /// <returns></returns>
        private ActionConnection CreateConnectionDisposeAction(SqlConnection sql_conn = null)
        {
            sql_conn = sql_conn ?? new SqlConnection (connection);

            if (sql_conn.State == ConnectionState.Closed)
                sql_conn.Open ();

            Action _act = () =>
            {
                if (sql_conn.State != System.Data.ConnectionState.Closed)
                {
                    try
                    {
                        sql_conn.Close ();
                    }
                    finally
                    {
                        sql_conn.Dispose ();
                    }
                }
            };

            return new ActionConnection () { act = _act, conn = sql_conn };
        }

        /// <summary>
        /// An internal class so that we can ensure that client code will not be able to alter the contents
        /// </summary>
        /// <typeparam name="T"></typeparam>
        private sealed class _InternalEnumerable<T> : IEnumerable<T>
        {

            public List<T> xs = new List<T> ();            

            public IEnumerator<T> GetEnumerator()
            {
                return xs.GetEnumerator ();
            }

            System.Collections.IEnumerator System.Collections.IEnumerable.GetEnumerator()
            {
                return GetEnumerator ();
            }
        }

        public void Save<T>(T x)
        {
            IEnumerable<T> xs = new[] { x };

            Save (xs);
        }


        class _InternalStringComparer : IEqualityComparer<string>
        {

            public bool Equals(string x, string y)
            {
                return x.Equals (y, StringComparison.OrdinalIgnoreCase);
            }

            public int GetHashCode(string obj)
            {
                return obj.GetHashCode ();
            }
        }


        /// <summary>
        /// Ostensibly, this method is for Creating or Updating a database record for each T in IEnumerable{T} however it can be used for any command that modifies the datastore and/or does not return a result set.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="xs"></param>
        public void Save<T>(IEnumerable<T> xs)
        {
            
            
            using (var conn = new SqlConnection(GetConnectionString()))
            {
                conn.Open ();

                var command = new SqlCommand (sql, conn);
                command.Parameters.AddRange (_params.ToArray ());

                var dict = this._params.ToDictionary (sp => sp.ParameterName.Substring (1));


                var explicityly_mapped_properties =
                                           from p in typeof (T).GetProperties ()
                                           let c = p.GetCustomAttributes (typeof (ColumnMappingAttribute), false)
                                           where c.Length > 0 && p.CanRead && dict.Keys.Contains (((ColumnMappingAttribute)c[0]).ColumnName)
                                           select new { ColumnName = ((ColumnMappingAttribute)c[0]).ColumnName, Property = p };

                var implicitly_mapped_properties =
                                        from p in typeof (T).GetProperties ()
                                        where p.GetCustomAttributes (typeof (ColumnMappingAttribute), false).Length == 0 && dict.Keys.Contains (p.Name) && p.CanRead
                                        select new { ColumnName = p.Name, Property = p };


                var properies = explicityly_mapped_properties.Union (implicitly_mapped_properties);

                if (properies.Count () < 1)
                    throw new Exception (string.Format ("The intersection of the query's parameter names and property names of type {0} is empty", typeof (T).FullName));

                foreach (var x in xs)
                {
                    foreach (var p in properies)
                    {
                        var val = p.Property.GetValue (x, null) ?? DBNull.Value;

                        dict[p.ColumnName].Value = val;
                    }

                    command.ExecuteNonQuery ();
                }

            }
        }


        class stringComparer : IEqualityComparer<string>
        {

            public bool Equals(string x, string y)
            {
                return x.Equals (y, StringComparison.OrdinalIgnoreCase);
            }

            public int GetHashCode(string obj)
            {
                return obj.GetHashCode ();
            }
        }


        /// <summary>
        /// Creates and populates an IEnumerabe{T} using the result set of a SQL querty
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <returns></returns>
        public IEnumerable<T> Instantiate<T>() where T : new()
        {
            var list = new _InternalEnumerable<T> ();

            using (var conn = new SqlConnection(GetConnectionString()))
            {
                conn.Open();

                var command = new SqlCommand(sql, conn);
                var rdr = command.ExecuteReader();

                var sw = new System.Diagnostics.Stopwatch ();

                sw.Start ();


                Action<T> act = null;

                if (typeof (T).IsSubclassOf (typeof (IDataInstantiable)))
                {
                    act = (x) => (x as IDataInstantiable).Hydrate (rdr);
                }
                else
                {
                    var count = rdr.FieldCount;
                    var names = new string[count];

                    var string_comparer = new stringComparer ();

                    for (var i = 0; i < count; i++)
                        names[i] = rdr.GetName (i);

                    var explicityly_mapped_properties =
                                            from p in typeof (T).GetProperties ()
                                            let c = p.GetCustomAttributes (typeof (ColumnMappingAttribute), false)
                                            where c.Length > 0 && p.CanWrite
                                            select new { ColumnName = ((ColumnMappingAttribute)c[0]).ColumnName, Property = p };

                    var implicitly_mapped_properties =
                                            from p in typeof (T).GetProperties ()
                                            where p.GetCustomAttributes (typeof (ColumnMappingAttribute), false).Length == 0 && names.Contains (p.Name, string_comparer) && p.CanWrite
                                            select new { ColumnName = p.Name, Property = p };

                    var properties = explicityly_mapped_properties.Union (implicitly_mapped_properties);


                    act = (x) =>
                        {

                            foreach (var p in properties)
                                p.Property.SetValue (x, rdr[p.ColumnName], null);
                        };
                }


                while (rdr.Read ())
                {
                    var o = new T ();

                    act (o);

                    list.xs.Add (o);
                }

                sw.Stop ();

                System.Diagnostics.Debug.WriteLine (sw.ElapsedMilliseconds);
            }

            return list;
        }

        System.Collections.IEnumerator System.Collections.IEnumerable.GetEnumerator()
        {
            return this.GetEnumerator ();
        }

        void IDisposable.Dispose()
        {
            var acts = dispose_acts;

            if (acts != null)
            {
                lock (this)
                {
                    if (acts != null)
                    {
                        acts ();
                    }
                }
            }
            
        }
    }
}
