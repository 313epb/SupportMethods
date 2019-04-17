using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.Data;
using System.Data.Common;
using System.Globalization;
using System.Linq;
using System.Reflection;
using System.Text;

namespace SupportMethods
{
    public static class SQL
    {
        public static readonly Dictionary<Type, DbType> TypeMap = new Dictionary<Type, DbType>
        {
            {typeof(byte),DbType.Byte},
            {typeof(sbyte),DbType.SByte},
            {typeof(short),DbType.Int16},
            {typeof(int),DbType.Int32},
            {typeof(uint),DbType.UInt32},
            {typeof(long),DbType.Int64},
            {typeof(ulong),DbType.UInt64},
            {typeof(float),DbType.Single},
            {typeof(double),DbType.Double},
            {typeof(decimal),DbType.Decimal},
            {typeof(bool),DbType.Boolean},
            {typeof(string),DbType.String},
            {typeof(char),DbType.StringFixedLength},
            {typeof(Guid),DbType.Guid},
            {typeof(DateTimeOffset),DbType.DateTimeOffset},
            {typeof(byte[]),DbType.Binary},
            {typeof(byte?),DbType.Byte},
            {typeof(sbyte?),DbType.SByte},
            {typeof(short?),DbType.Int16},
            {typeof(ushort?),DbType.UInt16},
            {typeof(int?),DbType.Int32},
            {typeof(uint?),DbType.UInt32},
            {typeof(long?),DbType.Int64},
            {typeof(ulong?),DbType.UInt64},
            {typeof(double?),DbType.Double},
            {typeof(decimal?),DbType.Decimal},
            {typeof(bool?),DbType.Boolean},
            {typeof(char?),DbType.StringFixedLength},
            {typeof(Guid?),DbType.Guid},
            {typeof(DateTime?),DbType.DateTime},
            {typeof(DateTimeOffset?),DbType.DateTimeOffset}
        };

        public static T DataToObject<T>(DbDataReader reader)
        {
            var result = (T)Activator.CreateInstance(typeof(T));
            var properties = typeof(T).GetProperties();
            foreach (var property in properties)
            {
                if (property.GetSetMethod() != null)
                {
                    var value = reader[property.Name];
                    if (value != DBNull.Value)
                    {
                        property.SetValue(result, value);
                    }
                }
            }
            return result;
        }

        public static string GetPropertiesNames<T>(string prefix = "")
        {
            StringBuilder result = new StringBuilder();
            var properties = typeof(T).GetProperties();
            foreach (var property in properties)
            {
                if (property.GetCustomAttributes().All(p => p.GetType() != typeof(Exclude)))
                {

                    if (string.IsNullOrEmpty(prefix))
                    {
                        result.Append(property.Name + ",");
                    }
                    else
                    {
                        result.Append(prefix + "." + property.Name + ",");
                    }
                }
            }
            result.Length--;
            return result.ToString();
        }
        public static string GetpropertiesValues<T>(T value)
        {
            StringBuilder result = new StringBuilder();
            var properties = typeof(T).GetProperties();
            foreach (var property in properties)
            {
                if (property.GetCustomAttributes().All(p => p.GetType() != typeof(Exclude)))
                {

                    if (property.PropertyType == typeof(string))
                    {
                        if (property.GetValue(value) != null)
                        {
                            result.Append("\'" + property.GetValue(value) + "\',");
                        }
                        else
                        {
                            result.Append("\'\',");
                        }
                    }
                    else if (property.PropertyType == typeof(DateTime) || property.PropertyType == typeof(DateTime?))
                    {
                        if (property.GetValue(value) != null)
                        {
                            CultureInfo culture = CultureInfo.CreateSpecificCulture("en-US");
                            DateTimeFormatInfo dtfi = culture.DateTimeFormat;
                            dtfi.DateSeparator = "/";
                            result.Append("\'" + ((DateTime)property.GetValue(value)).ToString("d", dtfi) + "\',");
                        }
                        else
                        {
                            result.Append("NULL,");
                        }
                    }
                    else if (property.PropertyType == typeof(decimal) || property.PropertyType == typeof(decimal?))
                    {
                        if (property.GetValue(value) != null)
                        {
                            result.Append(((decimal)property.GetValue(value)).ToString("G") + ",");
                        }
                        else
                        {
                            result.Append("0,");
                        }
                    }
                    else if (property.PropertyType == typeof(float) || property.PropertyType == typeof(float?))
                    {
                        if (property.GetValue(value) != null)
                        {
                            result.Append(((float)property.GetValue(value)).ToString("G") + "\'");
                        }
                        else
                        {
                            result.Append("0,");
                        }
                    }
                    else if (property.PropertyType == typeof(bool) || property.PropertyType == typeof(bool?))
                    {
                        if (property.GetValue(value) != null)
                        {
                            result.Append((bool)property.GetValue(value) ? "1," : "0,");
                        }
                        else
                        {
                            result.Append("NULL,");
                        }
                    }
                    else
                    {
                        if (property.GetValue(value) != null)
                        {
                            result.Append(property.GetValue(value) + ",");
                        }
                        else
                        {
                            result.Append("0,");
                        }
                    }
                }
            }
            result.Length--;
            return result.ToString();
        }

        public static string SelectCommand(string select, string from, string where = "")
        {
            StringBuilder result = new StringBuilder();
            result.Append("select " + (string.IsNullOrEmpty(select) ? "*" : select));
            result.Append(" from " + from);
            if (!string.IsNullOrEmpty(where)) result.Append(" where " + where);
            return result.ToString();
        }

        public static string InsertCommand(string tableName, string properties, string values)
        {
            StringBuilder result = new StringBuilder();
            result.Append("insert into " + tableName);
            result.Append(" (" + properties + ")");
            result.Append(" values (" + values + ");");
            return result.ToString();
        }

        public static string DeleteCommand(string tableName, string where)
        {
            return "delete from " + tableName + " where " + where;
        }

        public static void GenerateUpdateCommand<T>(DbCommand command, T value, string tableName, List<string> exclude, string where)
        {
            StringBuilder text = new StringBuilder();

            text.Append("Update " + tableName + " Set ");
            var properties = typeof(T).GetProperties();
            foreach (var property in properties)
            {
                if (!exclude.Contains(property.Name))
                {
                    text.Append(property.Name + "=@" + property.Name + ",");

                    var param = command.CreateParameter();
                    param.ParameterName = "@" + property.Name;
                    param.DbType = TypeMap[property.PropertyType];
                    param.Value = property.GetValue(value) == null ? DBNull.Value : property.GetValue(value);
                    command.Parameters.Add(param);
                }
            }
            text.Length--;
            text.Append(" where " + where);
            command.CommandText = text.ToString();
        }

        public static string InsertCommand<T>(string tableName)
        {
            StringBuilder text = new StringBuilder();

            text.Append("insert into " + tableName);

            StringBuilder str1 = new StringBuilder();
            StringBuilder str2 = new StringBuilder();

            var properties = typeof(T).GetProperties();
            foreach (var property in properties)
            {
                if (property.GetCustomAttributes().All(p => p.GetType() != typeof(Exclude)))
                {
                    str1.Append(" " + property.Name + ",");
                    str2.Append(" @" + property.Name + ",");
                }
            }
            str1.Length--;
            str2.Length--;
            text.Append(" (" + str1 + ") values (" + str2 + "); select cast(scope_identity() as int)");
            return text.ToString();
        }

        public static string UpdateCommand<T>(string tableName, T value, string where = "", List<string> exclude = null)
        {
            StringBuilder text = new StringBuilder();

            text.Append("update " + tableName + " set ");


            var properties = typeof(T).GetProperties();
            foreach (var property in properties)
            {
                if (property.GetCustomAttributes().All(p => p.GetType() != typeof(Exclude)))
                {
                    if (!exclude.Contains(property.Name))
                    {
                        text.Append(" " + property.Name + "=");

                        if (property.PropertyType == typeof(string))
                        {
                            if (property.GetValue(value) != null)
                            {
                                text.Append("\'" + property.GetValue(value) + "\',");
                            }
                            else
                            {
                                text.Append("\'\',");
                            }
                        }
                        else if (property.PropertyType == typeof(DateTime) || property.PropertyType == typeof(DateTime?))
                        {
                            if (property.GetValue(value) != null)
                            {
                                CultureInfo culture = CultureInfo.CreateSpecificCulture("en-US");
                                DateTimeFormatInfo dtfi = culture.DateTimeFormat;
                                dtfi.DateSeparator = "/";
                                text.Append("\'" + ((DateTime)property.GetValue(value)).ToString("d", dtfi) + "\',");
                            }
                            else
                            {
                                text.Append("NULL,");
                            }
                        }
                        else if (property.PropertyType == typeof(decimal) || property.PropertyType == typeof(decimal?))
                        {
                            if (property.GetValue(value) != null)
                            {
                                text.Append(((decimal)property.GetValue(value)).ToString("G") + ",");
                            }
                            else
                            {
                                text.Append("0,");
                            }
                        }
                        else if (property.PropertyType == typeof(float) || property.PropertyType == typeof(float?))
                        {
                            if (property.GetValue(value) != null)
                            {
                                text.Append(((float)property.GetValue(value)).ToString("G") + "\'");
                            }
                            else
                            {
                                text.Append("0,");
                            }
                        }
                        else if (property.PropertyType == typeof(bool) || property.PropertyType == typeof(bool?))
                        {
                            if (property.GetValue(value) != null)
                            {
                                text.Append((bool)property.GetValue(value) ? "1," : "0,");
                            }
                            else
                            {
                                text.Append("NULL,");
                            }
                        }
                        else
                        {
                            if (property.GetValue(value) != null)
                            {
                                text.Append(property.GetValue(value) + ",");
                            }
                            else
                            {
                                text.Append("0,");
                            }
                        }
                    }
                }
            }

            text.Length--;
            if (!string.IsNullOrEmpty(where))
            {
                text.Append(" where " + where);
            }
            return text.ToString();
        }

        public static string MergeCommand<T>(string tableName, List<T> values, List<string> exclude)
        {
            var result = new StringBuilder();
            result.Append("merge into " + tableName + "as Target using (values");
            var properties = typeof(T).GetProperties();
            foreach (var value in values)
            {
                result.Append(" (");
                foreach (var property in properties)
                {
                    if (property.GetCustomAttributes().All(p => p.GetType() != typeof(Exclude)))
                    {
                        if (!exclude.Contains(property.Name))
                        {
                            if (property.PropertyType == typeof(string))
                            {
                                if (property.GetValue(value) != null)
                                {
                                    result.Append("\'" + property.GetValue(value) + "\',");
                                }
                                else
                                {
                                    result.Append("\'\',");
                                }
                            }
                            else if (property.PropertyType == typeof(DateTime) || property.PropertyType == typeof(DateTime?))
                            {
                                if (property.GetValue(value) != null)
                                {
                                    CultureInfo culture = CultureInfo.CreateSpecificCulture("en-US");
                                    DateTimeFormatInfo dtfi = culture.DateTimeFormat;
                                    dtfi.DateSeparator = "/";
                                    result.Append("\'" + ((DateTime)property.GetValue(value)).ToString("d", dtfi) + "\',");
                                }
                                else
                                {
                                    result.Append("NULL,");
                                }
                            }
                            else if (property.PropertyType == typeof(decimal) || property.PropertyType == typeof(decimal?))
                            {
                                if (property.GetValue(value) != null)
                                {
                                    result.Append(((decimal)property.GetValue(value)).ToString("G") + ",");
                                }
                                else
                                {
                                    result.Append("0,");
                                }
                            }
                            else if (property.PropertyType == typeof(float) || property.PropertyType == typeof(float?))
                            {
                                if (property.GetValue(value) != null)
                                {
                                    result.Append(((float)property.GetValue(value)).ToString("G") + "\'");
                                }
                                else
                                {
                                    result.Append("0,");
                                }
                            }
                            else if (property.PropertyType == typeof(bool) || property.PropertyType == typeof(bool?))
                            {
                                if (property.GetValue(value) != null)
                                {
                                    result.Append((bool)property.GetValue(value) ? "1," : "0,");
                                }
                                else
                                {
                                    result.Append("NULL,");
                                }
                            }
                            else
                            {
                                if (property.GetValue(value) != null)
                                {
                                    result.Append(property.GetValue(value) + ",");
                                }
                                else
                                {
                                    result.Append("0,");
                                }
                            }
                        }
                    }
                }
                result.Append("),");
            }
            result.Length--;
            result.Append(") as Source (");
            foreach (var property in properties)
            {
                result.Append(property.Name + ",");
            }
            result.Length--;
            var key = properties.FirstOrDefault(p =>
                p.GetCustomAttributes().Any(a => a is KeyAttribute));

            result.Append(") on Target." + key.Name + "=Source." + key.Name);
            result.Append(" when not matched by Target then insert (");
            foreach (var property in properties)
            {
                result.Append(property.Name + ",");
            }
            result.Length--;
            result.Append(") values (");
            foreach (var property in properties)
            {
                result.Append(property.Name + ",");
            }
            result.Length--;
            result.Append(")");
            return result.ToString();
        }

    }
}
