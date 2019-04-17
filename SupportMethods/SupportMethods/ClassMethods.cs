using System;
using System.Text;

namespace SupportMethods
{
    public class ClassMethods
    {
        public override string ToString()
        {
            StringBuilder result = new StringBuilder();
            result.Append(GetType().Name + ":" + Environment.NewLine);
            var properties = GetType().GetProperties();
            foreach (var property in properties)
            {
                result.Append(property.Name + ": " + property.GetValue(this) + Environment.NewLine);
            }
            return result.ToString();
        }
    }
}