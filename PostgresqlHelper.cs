using Npgsql;
using System;
using System.Collections;
using System.Configuration;
using System.Data;

namespace Yx.Utility
{
    /// <summary>
    /// 数据库的通用访问代码
    /// 此类为抽象类，不允许实例化，在应用时直接调用即可
    /// </summary>
    public abstract class PostgresqlHelper
    {
        //获取数据库连接字符串，其属于静态变量且只读，项目中所有文档可以直接使用，但不能修改
        public static readonly string ConnectionStr = ConfigurationManager.ConnectionStrings["postgresql"].ConnectionString;

        // 哈希表用来存储缓存的参数信息，哈希表可以存储任意类型的参数。
        private static Hashtable parmCache = Hashtable.Synchronized(new Hashtable());

        #region 执行一个不需要返回值的NpgsqlCommand命令，通过指定专用的连接字符串
        /// <summary>
        /// 执行一个不需要返回值的NpgsqlCommand命令，通过指定专用的连接字符串。
        /// 使用参数数组形式提供参数列表 
        /// </summary>
        /// <remarks>
        /// 使用示例：
        ///  int result = ExecuteNonQuery(connString, CommandType.StoredProcedure, "PublishOrders", new NpgsqlParameter("@prodid", 24));
        /// </remarks>
        /// <param name="connectionString">一个有效的数据库连接字符串</param>
        /// <param name="commandType">NpgsqlCommand命令类型 (存储过程， T-SQL语句， 等等。)</param>
        /// <param name="commandText">存储过程的名字或者 T-SQL 语句</param>
        /// <param name="commandParameters">以数组形式提供NpgsqlCommand命令中用到的参数列表</param>
        /// <returns>返回一个数值表示此NpgsqlCommand命令执行后影响的行数</returns>
        public static int ExecuteNonQuery(string connectionString, CommandType cmdType, string cmdText, params NpgsqlParameter[] commandParameters)
        {

            var cmd = new NpgsqlCommand();
            using (var conn = new NpgsqlConnection(connectionString))
            {
                //通过PrePareCommand方法将参数逐个加入到NpgsqlCommand的参数集合中
                PrepareCommand(cmd, conn, null, cmdType, cmdText, commandParameters);
                int val = cmd.ExecuteNonQuery();

                //清空NpgsqlCommand中的参数列表
                cmd.Parameters.Clear();
                return val;
            }
        }
        #endregion

        #region 执行一条不返回结果的NpgsqlCommand，通过一个已经存在的数据库连接
        /// <summary>
        /// 执行一条不返回结果的NpgsqlCommand，通过一个已经存在的数据库连接 
        /// 使用参数数组提供参数
        /// </summary>
        /// <remarks>
        /// 使用示例：  
        ///  int result = ExecuteNonQuery(conn, CommandType.StoredProcedure, "PublishOrders", new NpgsqlParameter("@prodid", 24));
        /// </remarks>
        /// <param name="conn">一个现有的数据库连接</param>
        /// <param name="commandType">NpgsqlCommand命令类型 (存储过程， T-SQL语句， 等等。)</param>
        /// <param name="commandText">存储过程的名字或者 T-SQL 语句</param>
        /// <param name="commandParameters">以数组形式提供NpgsqlCommand命令中用到的参数列表</param>
        /// <returns>返回一个数值表示此NpgsqlCommand命令执行后影响的行数</returns>
        public static int ExecuteNonQuery(NpgsqlConnection connection, CommandType cmdType, string cmdText, params NpgsqlParameter[] commandParameters)
        {

            NpgsqlCommand cmd = new NpgsqlCommand();

            PrepareCommand(cmd, connection, null, cmdType, cmdText, commandParameters);
            int val = cmd.ExecuteNonQuery();
            cmd.Parameters.Clear();
            return val;
        }
        #endregion

        #region 执行一条不返回结果的NpgsqlCommand，通过一个已经存在的数据库事物处理 
        /// <summary>
        /// 执行一条不返回结果的NpgsqlCommand，通过一个已经存在的数据库事物处理 
        /// 使用参数数组提供参数
        /// </summary>
        /// <remarks>
        /// 使用示例： 
        ///  int result = ExecuteNonQuery(trans, CommandType.StoredProcedure, "PublishOrders", new NpgsqlParameter("@prodid", 24));
        /// </remarks>
        /// <param name="trans">一个存在的 sql 事物处理</param>
        /// <param name="commandType">NpgsqlCommand命令类型 (存储过程， T-SQL语句， 等等。)</param>
        /// <param name="commandText">存储过程的名字或者 T-SQL 语句</param>
        /// <param name="commandParameters">以数组形式提供NpgsqlCommand命令中用到的参数列表</param>
        /// <returns>返回一个数值表示此NpgsqlCommand命令执行后影响的行数</returns>
        public static int ExecuteNonQuery(NpgsqlTransaction trans, CommandType cmdType, string cmdText, params NpgsqlParameter[] commandParameters)
        {
            NpgsqlCommand cmd = new NpgsqlCommand();
            PrepareCommand(cmd, trans.Connection, trans, cmdType, cmdText, commandParameters);
            int val = cmd.ExecuteNonQuery();
            cmd.Parameters.Clear();
            return val;
        }
        #endregion

        #region 执行一条返回结果集的NpgsqlCommand命令，通过专用的连接字符串
        /// <summary>
        /// 执行一条返回结果集的NpgsqlCommand命令，通过专用的连接字符串。
        /// 使用参数数组提供参数
        /// </summary>
        /// <remarks>
        /// 使用示例：  
        ///  NpgsqlDataReader r = ExecuteReader(connString, CommandType.StoredProcedure, "PublishOrders", new NpgsqlParameter("@prodid", 24));
        /// </remarks>
        /// <param name="connectionString">一个有效的数据库连接字符串</param>
        /// <param name="commandType">NpgsqlCommand命令类型 (存储过程， T-SQL语句， 等等。)</param>
        /// <param name="commandText">存储过程的名字或者 T-SQL 语句</param>
        /// <param name="commandParameters">以数组形式提供NpgsqlCommand命令中用到的参数列表</param>
        /// <returns>返回一个包含结果的NpgsqlDataReader</returns>
        public static NpgsqlDataReader ExecuteReader(string connectionString, CommandType cmdType, string cmdText, params NpgsqlParameter[] commandParameters)
        {
            NpgsqlCommand cmd = new NpgsqlCommand();
            NpgsqlConnection conn = new NpgsqlConnection(connectionString);

            // 在这里使用try/catch处理是因为如果方法出现异常，则NpgsqlDataReader就不存在，
            //CommandBehavior.CloseConnection的语句就不会执行，触发的异常由catch捕获。
            //关闭数据库连接，并通过throw再次引发捕捉到的异常。
            try
            {
                PrepareCommand(cmd, conn, null, cmdType, cmdText, commandParameters);
                NpgsqlDataReader rdr = cmd.ExecuteReader(CommandBehavior.CloseConnection);
                cmd.Parameters.Clear();
                return rdr;
            }
            catch
            {
                conn.Close();
                throw;
            }
        }
        #endregion

        #region 执行一条返回第一条记录第一列的NpgsqlCommand命令，通过专用的连接字符串
        /// <summary>
        /// 执行一条返回第一条记录第一列的NpgsqlCommand命令，通过专用的连接字符串
        /// 使用参数数组提供参数
        /// </summary>
        /// <remarks>
        /// 使用示例：  
        ///  Object obj = ExecuteScalar(connString, CommandType.StoredProcedure, "PublishOrders", new NpgsqlParameter("@prodid", 24));
        /// </remarks>
        /// <param name="connectionString">一个有效的数据库连接字符串</param>
        /// <param name="commandType">NpgsqlCommand命令类型 (存储过程， T-SQL语句， 等等。)</param>
        /// <param name="commandText">存储过程的名字或者 T-SQL 语句</param>
        /// <param name="commandParameters">以数组形式提供NpgsqlCommand命令中用到的参数列表</param>
        /// <returns>返回一个object类型的数据，可以通过 Convert.To{Type}方法转换类型</returns>
        public static object ExecuteScalar(string connectionString, CommandType cmdType, string cmdText, params NpgsqlParameter[] commandParameters)
        {
            NpgsqlCommand cmd = new NpgsqlCommand();

            using (NpgsqlConnection connection = new NpgsqlConnection(connectionString))
            {
                PrepareCommand(cmd, connection, null, cmdType, cmdText, commandParameters);
                object val = cmd.ExecuteScalar();
                cmd.Parameters.Clear();
                return val;
            }
        }
        #endregion

        #region 执行一条返回第一条记录第一列的NpgsqlCommand命令，通过已经存在的数据库连接
        /// <summary>
        /// 执行一条返回第一条记录第一列的NpgsqlCommand命令，通过已经存在的数据库连接
        /// 使用参数数组提供参数
        /// </summary>
        /// <remarks>
        /// 使用示例： 
        ///  Object obj = ExecuteScalar(connString, CommandType.StoredProcedure, "PublishOrders", new NpgsqlParameter("@prodid", 24));
        /// </remarks>
        /// <param name="conn">一个已经存在的数据库连接</param>
        /// <param name="commandType">NpgsqlCommand命令类型 (存储过程， T-SQL语句， 等等。)</param>
        /// <param name="commandText">存储过程的名字或者 T-SQL 语句</param>
        /// <param name="commandParameters">以数组形式提供NpgsqlCommand命令中用到的参数列表</param>
        /// <returns>返回一个object类型的数据，可以通过 Convert.To{Type}方法转换类型</returns>
        public static object ExecuteScalar(NpgsqlConnection connection, CommandType cmdType, string cmdText, params NpgsqlParameter[] commandParameters)
        {
            NpgsqlCommand cmd = new NpgsqlCommand();

            PrepareCommand(cmd, connection, null, cmdType, cmdText, commandParameters);
            object val = cmd.ExecuteScalar();
            cmd.Parameters.Clear();
            return val;
        }
        #endregion

        #region 缓存参数数组
        /// <summary>
        /// 缓存参数数组
        /// </summary>
        /// <param name="cacheKey">参数缓存的键值</param>
        /// <param name="cmdParms">被缓存的参数列表</param>
        public static void CacheParameters(string cacheKey, params NpgsqlParameter[] commandParameters)
        {
            parmCache[cacheKey] = commandParameters;
        }
        #endregion

        #region 获取被缓存的参数
        /// <summary>
        /// 获取被缓存的参数
        /// </summary>
        /// <param name="cacheKey">用于查找参数的KEY值</param>
        /// <returns>返回缓存的参数数组</returns>
        public static NpgsqlParameter[] GetCachedParameters(string cacheKey)
        {
            NpgsqlParameter[] cachedParms = (NpgsqlParameter[])parmCache[cacheKey];

            if (cachedParms == null)
                return null;

            //新建一个参数的克隆列表
            NpgsqlParameter[] clonedParms = new NpgsqlParameter[cachedParms.Length];

            //通过循环为克隆参数列表赋值
            for (int i = 0, j = cachedParms.Length; i < j; i++)
                //使用clone方法复制参数列表中的参数
                clonedParms[i] = (NpgsqlParameter)((ICloneable)cachedParms[i]).Clone();

            return clonedParms;
        }
        #endregion

        #region 执行命令准备参数
        /// <summary>
        /// 为执行命令准备参数
        /// </summary>
        /// <param name="cmd">NpgsqlCommand 命令</param>
        /// <param name="conn">已经存在的数据库连接</param>
        /// <param name="trans">数据库事物处理</param>
        /// <param name="cmdType">NpgsqlCommand命令类型 (存储过程， T-SQL语句， 等等。)</param>
        /// <param name="cmdText">Command text，T-SQL语句 例如 Select * from Products</param>
        /// <param name="cmdParms">返回带参数的命令</param>
        private static void PrepareCommand(NpgsqlCommand cmd, NpgsqlConnection conn, NpgsqlTransaction trans, CommandType cmdType, string cmdText, NpgsqlParameter[] cmdParms)
        {

            //判断数据库连接状态
            if (conn.State != ConnectionState.Open)
                conn.Open();

            cmd.Connection = conn;
            cmd.CommandText = cmdText;

            //判断是否需要事物处理
            if (trans != null)
                cmd.Transaction = trans;

            cmd.CommandType = cmdType;

            if (cmdParms != null)
            {
                foreach (NpgsqlParameter parm in cmdParms)
                    cmd.Parameters.Add(parm);
            }
        }
        #endregion

        #region 执行查询，返回结果集中的第一行第一列的值，忽略其他行列
        /// <summary>
        /// 执行查询，返回结果集中的第一行第一列的值，忽略其他行列
        /// </summary>
        /// <param name="sql"></param>
        /// <returns></returns>
        public static object ExcuteScalar(string sql)
        {
            using (NpgsqlConnection con = new NpgsqlConnection(ConnectionStr))
            {
                con.Open();
                NpgsqlCommand cmd = new NpgsqlCommand(sql, con);
                con.Close();
                return cmd.ExecuteScalar();
            }
        }
        #endregion

        #region 执行查询
        /// <summary>
        /// 执行查询
        /// </summary>
        /// <param name="sql">有效的sql语句</param>
        /// <param name="param">返回DataReader</param>
        /// <returns>返回DataReader</returns>
        public static NpgsqlDataReader ExcuteReader(string sql, NpgsqlParameter[] param)
        {
            NpgsqlConnection con = new NpgsqlConnection(ConnectionStr);
            con.Open();
            NpgsqlCommand cmd = new NpgsqlCommand(sql, con);
            cmd.Parameters.AddRange(param);
            NpgsqlDataReader reader = cmd.ExecuteReader(CommandBehavior.CloseConnection);
            cmd.Parameters.Clear();
            return reader;
        }
        #endregion

        #region 执行查询
        /// <summary>
        /// 执行查询
        /// </summary>
        /// <param name="sql">有效的sql语句</param>
        /// <returns>返回DataReader</returns>
        public static NpgsqlDataReader ExcuteReader(string sql)
        {
            NpgsqlConnection con = new NpgsqlConnection(ConnectionStr);
            con.Open();
            NpgsqlCommand cmd = new NpgsqlCommand(sql, con);
            return cmd.ExecuteReader(CommandBehavior.CloseConnection);
        }
        #endregion

        #region 执行查询的基方法
        /// <summary>
        /// 执行查询的基方法
        /// </summary>
        /// <param name="sql">有效的sql语句</param>
        /// <returns>返回DataTable</returns>
        public static DataTable ExcuteDataQuery(string sql)
        {
            using (NpgsqlConnection con = new NpgsqlConnection(ConnectionStr))
            {
                con.Open();
                NpgsqlDataAdapter sda = new NpgsqlDataAdapter(sql, con);
                DataTable table = new DataTable();
                sda.Fill(table);
                con.Close();
                return table;
            }
        }
        #endregion

        #region 执行增，删，改的基方法
        /// <summary>
        /// 执行增，删，改的基方法
        /// </summary>
        /// <param name="sql">有效的sql语句</param>
        /// <param name="param">参数集合</param>
        /// <returns>影响的行数</returns>
        public static int ExcuteNonQuery(string sql, NpgsqlParameter[] param)
        {
            using (NpgsqlConnection con = new NpgsqlConnection(ConnectionStr))
            {
                con.Open();
                NpgsqlCommand cmd = new NpgsqlCommand(sql, con);
                if (param != null)
                {
                    cmd.Parameters.AddRange(param);
                }
                int count = cmd.ExecuteNonQuery();
                cmd.Parameters.Clear();
                con.Close();
                return count;
            }
        }
        #endregion

        #region 准备命令
        /// <summary>
        /// 准备命令
        /// </summary>
        /// <param name="con"></param>
        /// <param name="cmd"></param>
        /// <param name="textcmd"></param>
        /// <param name="cmdType"></param>
        /// <param name="param"></param>
        public static void PreparedCommd(NpgsqlConnection con, NpgsqlCommand cmd, string textcmd, CommandType cmdType, NpgsqlParameter[] param)
        {
            try
            {
                if (con.State != ConnectionState.Open)
                {
                    con.Open();
                }
                cmd.Connection = con;
                cmd.CommandText = textcmd;
                cmd.CommandType = cmdType;

                if (param != null)
                {
                    foreach (NpgsqlParameter p in param)
                    {
                        cmd.Parameters.Add(p);
                    }
                }
            }
            catch (Exception ex)
            {
                throw new Exception(ex.Message);
            }
        }
        #endregion

        #region 执行增、删、改
        /// <summary>
        /// 执行增、删、改
        /// </summary>
        /// <param name="textcmd">sql语句或者存储过程</param>
        /// <param name="cmdType">类型</param>
        /// <param name="param">参数</param>
        /// <returns>返回int类型的数据</returns>
        public static int ExecuteNonQuery(string textcmd, NpgsqlParameter[] param, CommandType cmdType)
        {
            using (NpgsqlConnection con = new NpgsqlConnection(ConnectionStr))
            {
                NpgsqlCommand cmd = new NpgsqlCommand();
                PreparedCommd(con, cmd, textcmd, cmdType, param);
                int num = cmd.ExecuteNonQuery();
                return num;
            }
        }
        #endregion

        #region 读取一行一列的数据
        /// <summary>
        /// 读取一行一列的数据
        /// </summary>
        /// <param name="textmd"></param>
        /// <param name="cmdType"></param>
        /// <param name="param"></param>
        /// <returns></returns>
        public static object ExecuteScalar(string textmd, CommandType cmdType, NpgsqlParameter[] param)
        {
            using (NpgsqlConnection con = new NpgsqlConnection(ConnectionStr))
            {
                NpgsqlCommand cmd = new NpgsqlCommand();
                PreparedCommd(con, cmd, textmd, cmdType, param);
                return cmd.ExecuteScalar();
            }
        }
        #endregion

        #region 读取一行一列的数据
        /// <summary>
        /// 读取一行一列的数据
        /// </summary>
        /// <param name="textmd"></param>
        /// <param name="cmdType"></param>
        /// <param name="param"></param>
        /// <returns></returns>
        public static object ExecuteScalar(string SQL)
        {
            using (NpgsqlConnection con = new NpgsqlConnection(ConnectionStr))
            {
                con.Open();
                NpgsqlCommand cmd = new NpgsqlCommand(SQL, con);
                return cmd.ExecuteScalar();
            }
        }
        #endregion

        #region 查询
        /// <summary>
        /// 查询
        /// </summary>
        /// <param name="textcmd"></param>
        /// <param name="cmdType"></param>
        /// <param name="param"></param>
        /// <returns></returns>
        public static NpgsqlDataReader ExecuteReader(string textcmd, CommandType cmdType, NpgsqlParameter[] param)
        {
            NpgsqlConnection con = new NpgsqlConnection(ConnectionStr);
            NpgsqlCommand cmd = new NpgsqlCommand();
            try
            {
                //PreparedCommd(con, cmd, textcmd, cmdType, param);
                NpgsqlDataReader read = cmd.ExecuteReader(CommandBehavior.CloseConnection);
                return read;
            }
            catch (Exception ex)
            {
                con.Close();
                throw new Exception(ex.Message);
            }
        }
        #endregion

        #region 查询返回DataTable
        /// <summary>
        /// 查询返回DataTable
        /// </summary>
        /// <param name="sql"></param>
        /// <returns></returns>
        public static DataTable ExecuteReader(string sql)
        {
            NpgsqlConnection con = new NpgsqlConnection(ConnectionStr);
            DataTable dt = new DataTable();
            try
            {
                NpgsqlDataAdapter da = new NpgsqlDataAdapter(sql, con);
                da.Fill(dt);
            }
            catch (Exception)
            {

                throw;
            }
            return dt;
        }
        #endregion
    }
}
