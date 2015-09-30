using Microsoft.SCOPE.Types;
using Microsoft.SCOPE.Interfaces;
using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using ScopeRuntime;

public class SessionIdGenerator : Reducer
{    
    public override Schema Produces(string[] columns, string[] args, Schema input)
    {
        Schema s = input.Clone();
        s.Add(new ColumnInfo("sessionId", ColumnDataType.Integer));
        return s;
    }
 
    public override IEnumerable<Row> Reduce(RowSet input, Row output, string[] args)
    {
        DateTime lastLogin = default(DateTime);
        int sessionId = 1;
        foreach (Row row in input.Rows)
        {
            DateTime currentLoginTime = (DateTime)row["logintime"].Value;
            if (lastLogin == default(DateTime))
            {
                lastLogin = currentLoginTime;
            }
            else
            {
                if (lastLogin.AddMinutes(30) < currentLoginTime)
                    sessionId++;

                lastLogin = currentLoginTime;
            }

            row.CopyTo(output);
            output["sessionId"].Set(sessionId);
            yield return output;

        }
    }
}
