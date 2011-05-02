using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Data.SqlClient;
using System.Configuration;
using System.Data;
using System.Dynamic;
using System.Linq.Expressions;


namespace Prelude
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
