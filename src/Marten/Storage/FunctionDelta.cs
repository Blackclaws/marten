﻿using System;
using Baseline;
using Marten.Schema;
using Marten.Util;

namespace Marten.Storage
{
    public class FunctionDelta
    {
        public FunctionBody Expected { get; set; }
        public FunctionBody Actual { get; set; }

        public FunctionDelta(FunctionBody expected, FunctionBody actual)
        {
            Expected = expected;
            Actual = actual;

            // TODO -- TEMP!!!!!
            if (expected.Function.Name == "mt_append_event")
            {
                var system = new FileSystem();
                system.WriteStringToFile("c:\\expected.sql", expected.Body.CanonicizeSql());
                system.WriteStringToFile("c:\\actual.sql", actual.Body.CanonicizeSql());
            }
        }

        public bool AllNew => Actual == null;
        public bool HasChanged => AllNew || !Expected.Body.CanonicizeSql().Equals(Actual.Body.CanonicizeSql(), StringComparison.OrdinalIgnoreCase);

        public void WritePatch(SchemaPatch patch)
        {
            if (AllNew)
            {
                patch.Updates.Apply(this, Expected.Body);

                Expected.DropStatements.Each(drop =>
                {
                    if (!drop.EndsWith("cascade", StringComparison.OrdinalIgnoreCase))
                    {
                        drop = drop.TrimEnd(';') + " cascade;";
                    }

                    patch.Rollbacks.Apply(this, drop);
                });

            }
            else if (HasChanged)
            {
                Actual.DropStatements.Each(drop =>
                {
                    if (!drop.EndsWith("cascade", StringComparison.OrdinalIgnoreCase))
                    {
                        drop = drop.TrimEnd(';') + " cascade;";
                    }

                    patch.Updates.Apply(this, drop);
                });

                patch.Updates.Apply(this, Expected.Body);

                Expected.DropStatements.Each(drop =>
                {
                    patch.Rollbacks.Apply(this, drop);
                });

            }
        }


        public override string ToString()
        {
            return Expected.Function.QualifiedName + " Diff";
        }
    }
}