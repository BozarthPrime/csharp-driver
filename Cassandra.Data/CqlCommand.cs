//
//      Copyright (C) 2012 DataStax Inc.
//
//   Licensed under the Apache License, Version 2.0 (the "License");
//   you may not use this file except in compliance with the License.
//   You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
//   Unless required by applicable law or agreed to in writing, software
//   distributed under the License is distributed on an "AS IS" BASIS,
//   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//   See the License for the specific language governing permissions and
//   limitations under the License.
//
/*
 * Updated 10-16-2013
 * Author: Joseph Bozarth
 * Description: Added better handeling for Scalar, support for execute dynamic and
 * dictionaries, and support for insert dynamics and dictionaries
 */
﻿using System;
using System.Collections;
using System.Collections.Generic;
using System.Text;
using System.Data;
using System.Data.Common;
using Cassandra;
using System.Threading;
using System.Dynamic;
using System.Linq;

namespace Cassandra.Data
{
    public sealed class CqlCommand : DbCommand
    {
        internal CqlConnection CqlConnection;
        internal CqlBatchTransaction CqlTransaction;
        private string commandText;

        public override void Cancel() { }

        public override string CommandText
        {
            get { return commandText; }
            set { commandText = value; }
        }

        public override int CommandTimeout
        {
            get { return Timeout.Infinite; }
            set { }
        }

        public override CommandType CommandType
        {
            get { return CommandType.Text; }
            set { }
        }

        protected override DbParameter CreateDbParameter()
        {
            throw new NotSupportedException();
        }

        protected override DbConnection DbConnection
        {
            get { return CqlConnection; }
            set
            {
                if (!(value is CqlConnection))
                    throw new InvalidOperationException();

                CqlConnection = (CqlConnection)value;
            }
        }

        protected override DbParameterCollection DbParameterCollection
        {
            get { throw new NotSupportedException(); }
        }

        protected override DbTransaction DbTransaction
        {
            get { return CqlTransaction; }
            set { CqlTransaction = (DbTransaction as CqlBatchTransaction); }
        }

        public override bool DesignTimeVisible
        {
            get { return true; }
            set { }
        }

        protected override DbDataReader ExecuteDbDataReader(CommandBehavior behavior)
        {
            var outp = CqlConnection.ManagedConnection.Execute(commandText);
            return new CqlReader(outp);
        }

        public override int ExecuteNonQuery()
        {
            var cm = commandText.ToUpper().TrimStart();
            if (cm.StartsWith("CREATE ")
                || cm.StartsWith("DROP ")
                || cm.StartsWith("ALTER "))
                CqlConnection.ManagedConnection.WaitForSchemaAgreement(CqlConnection.ManagedConnection.Execute(commandText));
            else
                CqlConnection.ManagedConnection.Execute(commandText);
            return -1;
        }

        # region Scalar Execution

        //public override object ExecuteScalar()
        //{
        //    return CqlConnection.ManagedConnection.Execute(commandText);
        //}

        public override object ExecuteScalar()
        {
            RowSet rows = CqlConnection.ManagedConnection.Execute(commandText);
            object result = null;

            if (rows.Columns.Length > 0)
            {
                var resultRows = rows.GetRows();
                var enumerator = resultRows.GetEnumerator();

                enumerator.MoveNext();

                Row row = enumerator.Current;

                if (row.Length > 0)
                    result = row.GetValue<object>(0);
            }

            return result;
        }

        # endregion


        # region Dynamic Execution

        public dynamic ExecuteDynamic()
        {
            dynamic toRet = null;

            List<dynamic> result = ExecuteDynamics();

            if (result != null && result.Count > 0)
                toRet = result[0];

            return toRet;
        }

        public List<dynamic> ExecuteDynamics()
        {
            List<dynamic> result = null;

            try
            {
                RowSet rows = CqlConnection.ManagedConnection.Execute(commandText);

                if (rows.Columns.Length > 0)
                {
                    result = new List<dynamic>();

                    foreach (Row row in rows.GetRows())
                    {
                        var cur = new ExpandoObject() as IDictionary<string, object>;

                        for (int i = 0; i < row.Length; i++)
                            cur.Add(row.GetName(i), row.GetValue<object>(i));

                        result.Add(cur);
                    }
                }
            }
            catch (Exception ex)
            {
                throw ex;
            }

            return result;
        }

        # endregion


        # region Dictionary Execution

        public Dictionary<string, object> ExecuteDictionary()
        {
            Dictionary<string, object> toRet = null;

            List<Dictionary<string, object>> result = ExecuteDictionaries();

            if (result != null && result.Count > 0)
                toRet = result[0];

            return toRet;
        }

        public List<Dictionary<string, object>> ExecuteDictionaries()
        {
            List<Dictionary<string, object>> result = null;

            try
            {
                RowSet rows = CqlConnection.ManagedConnection.Execute(commandText);

                if (rows.Columns.Length > 0)
                {
                    result = new List<Dictionary<string, object>>();

                    foreach (Row row in rows.GetRows())
                    {
                        Dictionary<string, object> cur = new Dictionary<string, object>();

                        for (int i = 0; i < row.Length; i++)
                            cur.Add(row.GetName(i), row.GetValue<object>(i));

                        result.Add(cur);
                    }
                }
            }
            catch (Exception ex)
            {
                throw ex;
            }

            return result;
        }

        # endregion 


        # region Insert Dynamics

        public int InsertDynamic(dynamic row, string columnFamily)
        {
            return InsertDynamicList(new List<dynamic> { row }, columnFamily);
        }

        public int InsertDynamicList(List<dynamic> rows, string columnFamily)
        {
            if (rows.Count > 0)
            {
                StringBuilder query = new StringBuilder("BEGIN BATCH ");

                foreach (dynamic row in rows)
                {
                    var cur = new ExpandoObject() as IDictionary<string, object>;
                    cur = row;

                    query.AppendFormat("INSERT INTO {0} ( {1}", columnFamily, cur.ElementAt(0).Key);

                    for (int i = 1; i < cur.Count; i++)
                    {
                        if (cur.ElementAt(i).Value != null)
                        {
                            query.AppendFormat(", {0}", cur.ElementAt(i).Key.ToLower());
                        }
                    }

                    Type curType = (cur.ElementAt(0).Value).GetType();
                    bool curIsString = curType == typeof(string);

                    query.AppendFormat(curIsString ? ") VALUES ( '{0}'" : ") VALUES ( {0}", cur.ElementAt(0).Value);

                    for (int j = 1; j < cur.Count; j++)
                    {
                        if (cur.ElementAt(j).Value != null)
                        {
                            curType = (cur.ElementAt(j).Value).GetType();
                            curIsString = curType == typeof(string);
                            query.AppendFormat(curIsString ? ", '{0}'" : ", {0}", cur.ElementAt(j).Value);
                        }
                    }

                    query.Append(") ");
                }

                query.Append(" APPLY BATCH;");

                commandText = query.ToString();

                CqlConnection.ManagedConnection.Execute(commandText);
            }

            return -1;
        }

        # endregion


        # region Insert Dictionaries

        public int InsertDictionary(Dictionary<string, object> row, string columnFamily)
        {
            return InsertDictionaryList(new List<Dictionary<string, object>> { row }, columnFamily);
        }

        public int InsertDictionaryList(List<Dictionary<string, object>> rows, string columnFamily)
        {
            if (rows.Count > 0)
            {
                StringBuilder query = new StringBuilder("BEGIN BATCH ");

                foreach (Dictionary<string, object> cur in rows)
                {
                    query.AppendFormat("INSERT INTO {0} ( {1}", columnFamily, cur.ElementAt(0).Key);

                    for (int i = 1; i < cur.Count; i++)
                    {
                        if (cur.ElementAt(i).Value != null)
                        {
                            query.AppendFormat(", {0}", cur.ElementAt(i).Key.ToLower());
                        }
                    }

                    Type curType = (cur.ElementAt(0).Value).GetType();
                    bool curIsString = curType == typeof(string);

                    query.AppendFormat(curIsString ? ") VALUES ( '{0}'" : ") VALUES ( {0}", cur.ElementAt(0).Value);

                    for (int j = 1; j < cur.Count; j++)
                    {
                        if (cur.ElementAt(j).Value != null)
                        {
                            curType = (cur.ElementAt(j).Value).GetType();
                            curIsString = curType == typeof(string);
                            query.AppendFormat(curIsString ? ", '{0}'" : ", {0}", cur.ElementAt(j).Value);
                        }
                    }

                    query.Append(") ");
                }

                query.Append(" APPLY BATCH;");

                commandText = query.ToString();

                CqlConnection.ManagedConnection.Execute(commandText);
            }

            return -1;
        }

        # endregion

        public override void Prepare()
        {
            throw new NotSupportedException();
        }

        public override UpdateRowSource UpdatedRowSource
        {
            get
            {
                return UpdateRowSource.FirstReturnedRecord;
            }
            set
            {
            }
        }
    }
}
