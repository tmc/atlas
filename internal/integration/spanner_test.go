// Copyright 2021-present The Atlas Authors. All rights reserved.
// This source code is licensed under the Apache 2.0 license found
// in the LICENSE file in the root directory of this source tree.

package integration

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"ariga.io/atlas/sql/migrate"
	"ariga.io/atlas/sql/schema"
	"ariga.io/atlas/sql/spanner"
	"ariga.io/atlas/sql/sqlclient"
	"entgo.io/ent/dialect"
	_ "github.com/lib/pq"
	"github.com/stretchr/testify/require"
)

type spannerTest struct {
	*testing.T
	db      *sql.DB
	drv     migrate.Driver
	rrw     migrate.RevisionReadWriter
	version string
	port    int
	once    sync.Once
}

var spannerTests = map[string]*spannerTest{
	"spanner-emulator": {port: 9020},
}

func stRun(t *testing.T, fn func(*spannerTest)) {
	for version, tt := range spannerTests {
		if flagVersion == "" || flagVersion == version {
			t.Run(version, func(t *testing.T) {
				tt.once.Do(func() {
					var err error
					tt.version = version
					tt.rrw = &rrw{}
					tt.db, err = sql.Open("spanner", "projects/atlas-dev/instances/instance-1/databases/db-1")
					if err != nil {
						log.Fatalln(err)
					}
					dbs = append(dbs, tt.db) // close connection after all tests have been run
					tt.drv, err = spanner.Open(tt.db)
					if err != nil {
						log.Fatalln(err)
					}
				})
				tt := &spannerTest{T: t, db: tt.db, drv: tt.drv, version: version, port: tt.port, rrw: tt.rrw}
				fn(tt)
			})
		}
	}
}

func TestSpanner_Executor(t *testing.T) {
	stRun(t, func(t *spannerTest) {
		testExecutor(t)
	})
}

func TestSpanner_AddDropTable(t *testing.T) {
	stRun(t, func(t *spannerTest) {
		testAddDrop(t)
	})
}

func TestSpanner_Relation(t *testing.T) {
	stRun(t, func(t *spannerTest) {
		testRelation(t)
	})
}

func TestSpanner_ColumnCheck(t *testing.T) {
	stRun(t, func(t *spannerTest) {
		usersT := &schema.Table{
			Name:  "users",
			Attrs: []schema.Attr{schema.NewCheck().SetName("users_c_check").SetExpr("c > 5")},
			Columns: []*schema.Column{
				{Name: "id", Type: &schema.ColumnType{Type: &schema.IntegerType{T: "int"}}},
				{Name: "c", Type: &schema.ColumnType{Type: &schema.IntegerType{T: "int"}}},
			},
		}
		t.dropTables(usersT.Name)
		t.migrate(&schema.AddTable{T: usersT})
		ensureNoChange(t, usersT)
	})
}

func TestSpanner_AddColumns(t *testing.T) {
	stRun(t, func(t *spannerTest) {
		usersT := t.users()
		t.dropTables(usersT.Name)
		t.migrate(&schema.AddTable{T: usersT})
		_, err := t.db.Exec("CREATE EXTENSION IF NOT EXISTS hstore")
		require.NoError(t, err)
		usersT.Columns = append(
			usersT.Columns,
			&schema.Column{Name: "a", Type: &schema.ColumnType{Type: &schema.BinaryType{T: "bytea"}}},
			&schema.Column{Name: "b", Type: &schema.ColumnType{Type: &schema.FloatType{T: "double precision", Precision: 10}}, Default: &schema.Literal{V: "10.1"}},
			&schema.Column{Name: "c", Type: &schema.ColumnType{Type: &schema.StringType{T: "character"}}, Default: &schema.Literal{V: "'y'"}},
			&schema.Column{Name: "d", Type: &schema.ColumnType{Type: &schema.DecimalType{T: "numeric", Precision: 10, Scale: 2}}, Default: &schema.Literal{V: "0.99"}},
			&schema.Column{Name: "e", Type: &schema.ColumnType{Type: &schema.JSONType{T: "json"}}, Default: &schema.Literal{V: "'{}'"}},
			&schema.Column{Name: "f", Type: &schema.ColumnType{Type: &schema.JSONType{T: "jsonb"}}, Default: &schema.Literal{V: "'1'"}},
			&schema.Column{Name: "g", Type: &schema.ColumnType{Type: &schema.FloatType{T: "float", Precision: 10}}, Default: &schema.Literal{V: "'1'"}},
			&schema.Column{Name: "h", Type: &schema.ColumnType{Type: &schema.FloatType{T: "float", Precision: 30}}, Default: &schema.Literal{V: "'1'"}},
			&schema.Column{Name: "i", Type: &schema.ColumnType{Type: &schema.FloatType{T: "float", Precision: 53}}, Default: &schema.Literal{V: "1"}},
			&schema.Column{Name: "m", Type: &schema.ColumnType{Type: &schema.BoolType{T: "boolean"}, Null: true}, Default: &schema.Literal{V: "false"}},
			&schema.Column{Name: "n", Type: &schema.ColumnType{Type: &schema.SpatialType{T: "point"}, Null: true}, Default: &schema.Literal{V: "'(1,2)'"}},
			&schema.Column{Name: "o", Type: &schema.ColumnType{Type: &schema.SpatialType{T: "line"}, Null: true}, Default: &schema.Literal{V: "'{1,2,3}'"}},
		)
		changes := t.diff(t.loadUsers(), usersT)
		require.Len(t, changes, 17)
		t.migrate(&schema.ModifyTable{T: usersT, Changes: changes})
		ensureNoChange(t, usersT)
	})
}

func TestSpanner_ColumnInt(t *testing.T) {
	ctx := context.Background()
	run := func(t *testing.T, change func(*schema.Column)) {
		stRun(t, func(t *spannerTest) {
			usersT := &schema.Table{
				Name:    "users",
				Columns: []*schema.Column{{Name: "a", Type: &schema.ColumnType{Type: &schema.IntegerType{T: "bigint"}}}},
			}
			err := t.drv.ApplyChanges(ctx, []schema.Change{&schema.AddTable{T: usersT}})
			require.NoError(t, err)
			t.dropTables(usersT.Name)
			change(usersT.Columns[0])
			changes := t.diff(t.loadUsers(), usersT)
			require.Len(t, changes, 1)
			t.migrate(&schema.ModifyTable{T: usersT, Changes: changes})
			ensureNoChange(t, usersT)
		})
	}

	t.Run("ChangeNull", func(t *testing.T) {
		run(t, func(c *schema.Column) {
			c.Type.Null = true
		})
	})

	t.Run("ChangeType", func(t *testing.T) {
		run(t, func(c *schema.Column) {
			c.Type.Type.(*schema.IntegerType).T = "integer"
		})
	})

	t.Run("ChangeDefault", func(t *testing.T) {
		run(t, func(c *schema.Column) {
			c.Default = &schema.RawExpr{X: "0"}
		})
	})
}

func TestSpanner_ColumnArray(t *testing.T) {
	stRun(t, func(t *spannerTest) {
		usersT := t.users()
		t.dropTables(usersT.Name)
		t.migrate(&schema.AddTable{T: usersT})

		// Add column.
		usersT.Columns = append(
			usersT.Columns,
			&schema.Column{Name: "a", Type: &schema.ColumnType{Raw: "int[]", Type: &spanner.ArrayType{Type: &schema.IntegerType{T: "int"}, T: "int[]"}}, Default: &schema.Literal{V: "'{1}'"}},
		)
		changes := t.diff(t.loadUsers(), usersT)
		require.Len(t, changes, 1)
		t.migrate(&schema.ModifyTable{T: usersT, Changes: changes})
		ensureNoChange(t, usersT)

		// Check default.
		usersT.Columns[2].Default = &schema.RawExpr{X: "ARRAY[1]"}
		ensureNoChange(t, usersT)

		// Change default.
		usersT.Columns[2].Default = &schema.RawExpr{X: "ARRAY[1,2]"}
		changes = t.diff(t.loadUsers(), usersT)
		require.Len(t, changes, 1)
		t.migrate(&schema.ModifyTable{T: usersT, Changes: changes})
		ensureNoChange(t, usersT)
	})
}

func TestSpanner_Enums(t *testing.T) {
	stRun(t, func(t *spannerTest) {
		ctx := context.Background()
		usersT := &schema.Table{
			Name:   "users",
			Schema: t.realm().Schemas[0],
			Columns: []*schema.Column{
				{Name: "state", Type: &schema.ColumnType{Type: &schema.EnumType{T: "state", Values: []string{"on", "off"}}}},
			},
		}
		t.Cleanup(func() {
			_, err := t.drv.ExecContext(ctx, "DROP TYPE IF EXISTS state, day")
			require.NoError(t, err)
		})

		// Create table with an enum column.
		err := t.drv.ApplyChanges(ctx, []schema.Change{&schema.AddTable{T: usersT}})
		require.NoError(t, err, "create a new table with an enum column")
		t.dropTables(usersT.Name)
		ensureNoChange(t, usersT)

		// Add another enum column.
		usersT.Columns = append(
			usersT.Columns,
			&schema.Column{Name: "day", Type: &schema.ColumnType{Type: &schema.EnumType{T: "day", Values: []string{"sunday", "monday"}}}},
		)
		changes := t.diff(t.loadUsers(), usersT)
		require.Len(t, changes, 1)
		err = t.drv.ApplyChanges(ctx, []schema.Change{&schema.ModifyTable{T: usersT, Changes: changes}})
		require.NoError(t, err, "add a new enum column to existing table")
		ensureNoChange(t, usersT)

		// Add a new value to an existing enum.
		e := usersT.Columns[1].Type.Type.(*schema.EnumType)
		e.Values = append(e.Values, "tuesday")
		changes = t.diff(t.loadUsers(), usersT)
		require.Len(t, changes, 1)
		err = t.drv.ApplyChanges(ctx, []schema.Change{&schema.ModifyTable{T: usersT, Changes: changes}})
		require.NoError(t, err, "append a value to existing enum")
		ensureNoChange(t, usersT)

		// Add multiple new values to an existing enum.
		e = usersT.Columns[1].Type.Type.(*schema.EnumType)
		e.Values = append(e.Values, "wednesday", "thursday", "friday", "saturday")
		changes = t.diff(t.loadUsers(), usersT)
		require.Len(t, changes, 1)
		err = t.drv.ApplyChanges(ctx, []schema.Change{&schema.ModifyTable{T: usersT, Changes: changes}})
		require.NoError(t, err, "append multiple values to existing enum")
		ensureNoChange(t, usersT)
	})
}

func TestSpanner_ForeignKey(t *testing.T) {
	t.Run("ChangeAction", func(t *testing.T) {
		stRun(t, func(t *spannerTest) {
			usersT, postsT := t.users(), t.posts()
			t.dropTables(postsT.Name, usersT.Name)
			t.migrate(&schema.AddTable{T: usersT}, &schema.AddTable{T: postsT})
			ensureNoChange(t, postsT, usersT)

			postsT = t.loadPosts()
			fk, ok := postsT.ForeignKey("author_id")
			require.True(t, ok)
			fk.OnUpdate = schema.SetNull
			fk.OnDelete = schema.Cascade
			changes := t.diff(t.loadPosts(), postsT)
			require.Len(t, changes, 1)
			modifyF, ok := changes[0].(*schema.ModifyForeignKey)
			require.True(t, ok)
			require.True(t, modifyF.Change == schema.ChangeUpdateAction|schema.ChangeDeleteAction)

			t.migrate(&schema.ModifyTable{T: postsT, Changes: changes})
			ensureNoChange(t, postsT, usersT)
		})
	})

	t.Run("UnsetNull", func(t *testing.T) {
		stRun(t, func(t *spannerTest) {
			usersT, postsT := t.users(), t.posts()
			t.dropTables(postsT.Name, usersT.Name)
			fk, ok := postsT.ForeignKey("author_id")
			require.True(t, ok)
			fk.OnDelete = schema.SetNull
			fk.OnUpdate = schema.SetNull
			t.migrate(&schema.AddTable{T: usersT}, &schema.AddTable{T: postsT})
			ensureNoChange(t, postsT, usersT)

			postsT = t.loadPosts()
			c, ok := postsT.Column("author_id")
			require.True(t, ok)
			c.Type.Null = false
			fk, ok = postsT.ForeignKey("author_id")
			require.True(t, ok)
			fk.OnUpdate = schema.NoAction
			fk.OnDelete = schema.NoAction
			changes := t.diff(t.loadPosts(), postsT)
			require.Len(t, changes, 2)
			modifyC, ok := changes[0].(*schema.ModifyColumn)
			require.True(t, ok)
			require.True(t, modifyC.Change == schema.ChangeNull)
			modifyF, ok := changes[1].(*schema.ModifyForeignKey)
			require.True(t, ok)
			require.True(t, modifyF.Change == schema.ChangeUpdateAction|schema.ChangeDeleteAction)

			t.migrate(&schema.ModifyTable{T: postsT, Changes: changes})
			ensureNoChange(t, postsT, usersT)
		})
	})

	t.Run("AddDrop", func(t *testing.T) {
		stRun(t, func(t *spannerTest) {
			usersT := t.users()
			t.dropTables(usersT.Name)
			t.migrate(&schema.AddTable{T: usersT})
			ensureNoChange(t, usersT)

			// Add foreign key.
			usersT.Columns = append(usersT.Columns, &schema.Column{
				Name: "spouse_id",
				Type: &schema.ColumnType{Raw: "bigint", Type: &schema.IntegerType{T: "bigint"}, Null: true},
			})
			usersT.ForeignKeys = append(usersT.ForeignKeys, &schema.ForeignKey{
				Symbol:     "spouse_id",
				Table:      usersT,
				Columns:    usersT.Columns[len(usersT.Columns)-1:],
				RefTable:   usersT,
				RefColumns: usersT.Columns[:1],
				OnDelete:   schema.NoAction,
			})

			changes := t.diff(t.loadUsers(), usersT)
			require.Len(t, changes, 2)
			addC, ok := changes[0].(*schema.AddColumn)
			require.True(t, ok)
			require.Equal(t, "spouse_id", addC.C.Name)
			addF, ok := changes[1].(*schema.AddForeignKey)
			require.True(t, ok)
			require.Equal(t, "spouse_id", addF.F.Symbol)
			t.migrate(&schema.ModifyTable{T: usersT, Changes: changes})
			ensureNoChange(t, usersT)

			// Drop foreign keys.
			usersT.Columns = usersT.Columns[:len(usersT.Columns)-1]
			usersT.ForeignKeys = usersT.ForeignKeys[:len(usersT.ForeignKeys)-1]
			changes = t.diff(t.loadUsers(), usersT)
			require.Len(t, changes, 2)
			t.migrate(&schema.ModifyTable{T: usersT, Changes: changes})
			ensureNoChange(t, usersT)
		})
	})
}

func TestSpanner_Ent(t *testing.T) {
	stRun(t, func(t *spannerTest) {
		testEntIntegration(t, dialect.Postgres, t.db)
	})
	// Migration to global unique identifiers.
	t.Run("GlobalUniqueID", func(t *testing.T) {
		stRun(t, func(t *spannerTest) {
			ctx := context.Background()
			t.dropTables("global_id")
			_, err := t.driver().ExecContext(ctx, "CREATE TABLE global_id (id int NOT NULL GENERATED BY DEFAULT AS IDENTITY, PRIMARY KEY(id))")
			require.NoError(t, err)
			_, err = t.driver().ExecContext(ctx, "ALTER TABLE global_id ALTER COLUMN id RESTART WITH 1024")
			require.NoError(t, err)
			_, err = t.driver().ExecContext(ctx, "INSERT INTO global_id VALUES (default), (default)")
			require.NoError(t, err)
			var id int
			require.NoError(t, t.db.QueryRow("SELECT id FROM global_id").Scan(&id))
			require.Equal(t, 1024, id)
			_, err = t.driver().ExecContext(ctx, "DELETE FROM global_id WHERE id = 1024")
			require.NoError(t, err)

			globalT := t.loadTable("global_id")
			_, ok := globalT.Column("id")
			require.True(t, ok)
			t.migrate(&schema.ModifyTable{
				T: globalT,
				Changes: []schema.Change{
					&schema.ModifyColumn{
						From:   globalT.Columns[0],
						To:     schema.NewIntColumn("id", "int"),
						Change: schema.ChangeAttr,
					},
				},
			})
			_, err = t.driver().ExecContext(ctx, "INSERT INTO global_id VALUES (default), (default)")
			require.NoError(t, err)
			globalT = t.loadTable("global_id")
			_, ok = globalT.Column("id")
			require.True(t, ok)
		})
	})
}

func TestSpanner_AdvisoryLock(t *testing.T) {
	stRun(t, func(t *spannerTest) {
		testAdvisoryLock(t.T, t.drv.(schema.Locker))
	})
}

func TestSpanner_HCL(t *testing.T) {
	full := `
schema "public" {
}
table "users" {
	schema = schema.public
	column "id" {
		type = int
	}
	primary_key {
		columns = [table.users.column.id]
	}
}
table "posts" {
	schema = schema.public
	column "id" {
		type = int
	}
	column "tags" {
		type = sql("text[]")
	}
	column "author_id" {
		type = int
	}
	foreign_key "author" {
		columns = [
			table.posts.column.author_id,
		]
		ref_columns = [
			table.users.column.id,
		]
	}
	primary_key {
		columns = [table.users.column.id]
	}
}
`
	empty := `
schema "public" {
}
`
	stRun(t, func(t *spannerTest) {
		testHCLIntegration(t, full, empty)
	})
}

func TestSpanner_HCL_Realm(t *testing.T) {
	stRun(t, func(t *spannerTest) {
		t.dropSchemas("second")
		realm := t.loadRealm()
		hcl, err := spanner.MarshalHCL(realm)
		require.NoError(t, err)
		wa := string(hcl) + `
schema "second" {
}
`
		t.applyRealmHcl(wa)
		realm, err = t.drv.InspectRealm(context.Background(), &schema.InspectRealmOption{})
		require.NoError(t, err)
		_, ok := realm.Schema("public")
		require.True(t, ok)
		_, ok = realm.Schema("second")
		require.True(t, ok)
	})
}

func TestSpanner_HCL_ForeignKeyCrossSchema(t *testing.T) {
	const expected = `table "credit_cards" {
  schema = schema.financial
  column "id" {
    null = false
    type = serial
  }
  column "user_id" {
    null = false
    type = integer
  }
  primary_key {
    columns = [column.id]
  }
  foreign_key "user_id_fkey" {
    columns     = [column.user_id]
    ref_columns = [table.users.users.column.id]
    on_update   = NO_ACTION
    on_delete   = NO_ACTION
  }
}
table "financial" "users" {
  schema = schema.financial
  column "id" {
    null = false
    type = serial
  }
}
table "users" "users" {
  schema = schema.users
  column "id" {
    null = false
    type = bigserial
  }
  column "email" {
    null = false
    type = character_varying
  }
  primary_key {
    columns = [column.id]
  }
}
schema "financial" {
}
schema "users" {
}
`
	stRun(t, func(t *spannerTest) {
		// t.dropSchemas("financial", "users")
		// realm := t.loadRealm()
		// hcl, err := spanner.MarshalHCL(realm)
		// require.NoError(t, err)
		// t.applyRealmHcl(string(hcl) + "\n" + expected)
		// realm, err = t.drv.InspectRealm(context.Background(), &schema.InspectRealmOption{Schemas: []string{"users", "financial"}})
		// require.NoError(t, err)
		// actual, err := spanner.MarshalHCL(realm)
		// require.NoError(t, err)
		// require.Equal(t, expected, string(actual))
	})
}

func (t *spannerTest) applyRealmHcl(spec string) {
	// realm := t.loadRealm()
	// var desired schema.Realm
	// err := spanner.EvalHCLBytes([]byte(spec), &desired, nil)
	// require.NoError(t, err)
	// diff, err := t.drv.RealmDiff(realm, &desired)
	// require.NoError(t, err)
	// err = t.drv.ApplyChanges(context.Background(), diff)
	// require.NoError(t, err)
}

func TestSpanner_Snapshot(t *testing.T) {
	stRun(t, func(t *spannerTest) {
		client, err := sqlclient.Open(context.Background(), fmt.Sprintf("spanner://spanner:pass@localhost:%d/test?sslmode=disable&search_path=another", t.port))
		require.NoError(t, err)

		_, err = client.ExecContext(context.Background(), "CREATE SCHEMA another")
		require.NoError(t, err)
		t.Cleanup(func() {
			_, err = client.ExecContext(context.Background(), "DROP SCHEMA IF EXISTS another")
			require.NoError(t, client.Close())
		})
		drv := client.Driver

		_, err = t.driver().(migrate.Snapshoter).Snapshot(context.Background())
		require.ErrorAs(t, err, &migrate.NotCleanError{})

		r, err := drv.InspectRealm(context.Background(), nil)
		require.NoError(t, err)
		restore, err := drv.(migrate.Snapshoter).Snapshot(context.Background())
		require.NoError(t, err) // connected to test schema
		require.NoError(t, drv.ApplyChanges(context.Background(), []schema.Change{
			&schema.AddTable{T: schema.NewTable("my_table").
				AddColumns(
					schema.NewIntColumn("col_1", "integer").SetNull(true),
					schema.NewIntColumn("col_2", "bigint"),
				),
			},
		}))
		t.Cleanup(func() {
			t.dropTables("my_table")
		})
		require.NoError(t, restore(context.Background()))
		r1, err := drv.InspectRealm(context.Background(), nil)
		require.NoError(t, err)
		diff, err := drv.RealmDiff(r1, r)
		require.NoError(t, err)
		require.Zero(t, diff)
	})
}

func TestSpanner_CLI_MigrateApplyBC(t *testing.T) {
	stRun(t, func(t *spannerTest) {
		testCLIMigrateApplyBC(t, "spanner")
	})
}

func TestSpanner_CLI(t *testing.T) {
	h := `
			schema "public" {
			}
			table "users" {
				schema = schema.public
				column "id" {
					type = integer
				}
				primary_key {
					columns = [table.users.column.id]
				}
			}`
	t.Run("SchemaInspect", func(t *testing.T) {
		stRun(t, func(t *spannerTest) {
			testCLISchemaInspect(t, h, t.url(""), spanner.EvalHCL)
		})
	})
	t.Run("SchemaApply", func(t *testing.T) {
		stRun(t, func(t *spannerTest) {
			testCLISchemaApply(t, h, t.url(""))
		})
	})
	t.Run("SchemaApplyDryRun", func(t *testing.T) {
		stRun(t, func(t *spannerTest) {
			testCLISchemaApplyDry(t, h, t.url(""))
		})
	})
	t.Run("SchemaApplyWithVars", func(t *testing.T) {
		h := `
variable "tenant" {
	type = string
}
schema "tenant" {
	name = var.tenant
}
table "users" {
	schema = schema.tenant
	column "id" {
		type = int
	}
}
`
		stRun(t, func(t *spannerTest) {
			testCLISchemaApply(t, h, t.url(""), "--var", "tenant=public")
		})
	})
	t.Run("SchemaDiffRun", func(t *testing.T) {
		stRun(t, func(t *spannerTest) {
			testCLISchemaDiff(t, t.url(""))
		})
	})
	t.Run("SchemaApplyAutoApprove", func(t *testing.T) {
		stRun(t, func(t *spannerTest) {
			testCLISchemaApplyAutoApprove(t, h, t.url(""))
		})
	})
}

func TestSpanner_CLI_MultiSchema(t *testing.T) {
	h := `
			schema "public" {	
			}
			table "users" {
				schema = schema.public
				column "id" {
					type = integer
				}
				primary_key {
					columns = [table.users.column.id]
				}
			}
			schema "test2" {	
			}
			table "users" {
				schema = schema.test2
				column "id" {
					type = integer
				}
				primary_key {
					columns = [table.users.column.id]
				}
			}`
	t.Run("SchemaInspect", func(t *testing.T) {
		stRun(t, func(t *spannerTest) {
			t.dropSchemas("test2")
			t.dropTables("users")
			testCLIMultiSchemaInspect(t, h, t.url(""), []string{"public", "test2"}, spanner.EvalHCL)
		})
	})
	t.Run("SchemaApply", func(t *testing.T) {
		stRun(t, func(t *spannerTest) {
			t.dropSchemas("test2")
			t.dropTables("users")
			testCLIMultiSchemaApply(t, h, t.url(""), []string{"public", "test2"}, spanner.EvalHCL)
		})
	})
}

func TestSpanner_MigrateDiffRealm(t *testing.T) {
	bin, err := buildCmd(t)
	require.NoError(t, err)
	stRun(t, func(t *spannerTest) {
		dir := t.TempDir()
		_, err := t.db.Exec("CREATE DATABASE migrate_diff")
		require.NoError(t, err)
		defer t.db.Exec("DROP DATABASE IF EXISTS migrate_diff")

		hcl := `
schema "public" {}
table "users" {
	schema = schema.public
	column "id" { type = integer }
}
schema "other" {}
table "posts" {
	schema = schema.other
	column "id" { type = integer }
}
`
		err = os.WriteFile(filepath.Join(dir, "schema.hcl"), []byte(hcl), 0600)
		diff := func(name string) string {
			out, err := exec.Command(
				bin, "migrate", "diff", name,
				"--dir", fmt.Sprintf("file://%s", filepath.Join(dir, "migrations")),
				"--to", fmt.Sprintf("file://%s", filepath.Join(dir, "schema.hcl")),
				"--dev-url", fmt.Sprintf("spanner://spanner:pass@localhost:%d/migrate_diff?sslmode=disable", t.port),
			).CombinedOutput()
			require.NoError(t, err, string(out))
			return strings.TrimSpace(string(out))
		}
		require.Empty(t, diff("initial"))

		// Expect one file and read its contents.
		files, err := os.ReadDir(filepath.Join(dir, "migrations"))
		require.NoError(t, err)
		require.Equal(t, 2, len(files))
		require.Equal(t, "atlas.sum", files[1].Name())
		b, err := os.ReadFile(filepath.Join(dir, "migrations", files[0].Name()))
		require.NoError(t, err)
		require.Equal(t,
			`-- Add new schema named "other"
CREATE SCHEMA "other";
-- create "users" table
CREATE TABLE "public"."users" ("id" integer NOT NULL);
-- create "posts" table
CREATE TABLE "other"."posts" ("id" integer NOT NULL);
`, string(b))
		require.Equal(t, "The migration directory is synced with the desired state, no changes to be made", diff("no_change"))

		// Append a change to the schema and expect a migration to be created.
		hcl += `
table "other" "users" {
	schema = schema.other
	column "id" { type = integer }
}`
		err = os.WriteFile(filepath.Join(dir, "schema.hcl"), []byte(hcl), 0600)
		require.Empty(t, diff("second"))
		require.Equal(t, "The migration directory is synced with the desired state, no changes to be made", diff("no_change"))
		files, err = os.ReadDir(filepath.Join(dir, "migrations"))
		require.NoError(t, err)
		require.Equal(t, 3, len(files), dir)
		b, err = os.ReadFile(filepath.Join(dir, "migrations", files[1].Name()))
		require.NoError(t, err)
		require.Equal(t,
			`-- create "users" table
CREATE TABLE "other"."users" ("id" integer NOT NULL);
`, string(b))
	})
}

func TestSpanner_SchemaDiff(t *testing.T) {
	bin, err := buildCmd(t)
	require.NoError(t, err)
	stRun(t, func(t *spannerTest) {
		dir := t.TempDir()
		_, err = t.db.Exec("CREATE DATABASE test1")
		require.NoError(t, err)
		t.Cleanup(func() {
			_, err := t.db.Exec("DROP DATABASE IF EXISTS test1")
			require.NoError(t, err)
		})
		_, err = t.db.Exec("CREATE DATABASE test2")
		require.NoError(t, err)
		t.Cleanup(func() {
			_, err = t.db.Exec("DROP DATABASE IF EXISTS test2")
			require.NoError(t, err)
		})

		diff := func(db1, db2 string) string {
			out, err := exec.Command(
				bin, "schema", "diff",
				"--from", fmt.Sprintf("spanner://spanner:pass@localhost:%d/%s", t.port, db1),
				"--to", fmt.Sprintf("spanner://spanner:pass@localhost:%d/%s", t.port, db2),
			).CombinedOutput()
			require.NoError(t, err, string(out))
			return strings.TrimSpace(string(out))
		}
		// Diff a database with itself.
		require.Equal(t, "Schemas are synced, no changes to be made.", diff("test1?sslmode=disable", "test2?sslmode=disable"))

		// Create schemas on test2 database.
		hcl := `
schema "public" {}
table "users" {
	schema = schema.public
	column "id" { type = integer }
}
schema "other" {}
table "posts" {
	schema = schema.other
	column "id" { type = integer }
}
`
		err = os.WriteFile(filepath.Join(dir, "schema.hcl"), []byte(hcl), 0600)
		require.NoError(t, err)
		out, err := exec.Command(
			bin, "schema", "apply",
			"-u", fmt.Sprintf("spanner://spanner:pass@localhost:%d/test2?sslmode=disable", t.port),
			"-f", fmt.Sprintf(filepath.Join(dir, "schema.hcl")),
			"--auto-approve",
		).CombinedOutput()
		require.NoError(t, err, string(out))

		// Diff a database with different one.
		require.Equal(t, `-- Add new schema named "other"
CREATE SCHEMA "other"
-- Create "users" table
CREATE TABLE "public"."users" ("id" integer NOT NULL)
-- Create "posts" table
CREATE TABLE "other"."posts" ("id" integer NOT NULL)`, diff("test1?sslmode=disable", "test2?sslmode=disable"))
		// diff schemas
		require.Equal(t, `-- Drop "posts" table
DROP TABLE "posts"
-- Create "users" table
CREATE TABLE "users" ("id" integer NOT NULL)`, diff("test2?sslmode=disable&search_path=other", "test2?sslmode=disable&search_path=public"))
		// diff between schema and database
		out, err = exec.Command(
			bin, "schema", "diff",
			"--from", fmt.Sprintf("spanner://spanner:pass@localhost:%d/test2?sslmode=disable", t.port),
			"--to", fmt.Sprintf("spanner://spanner:pass@localhost:%d/test2?sslmode=disable&search_path=public", t.port),
		).CombinedOutput()
		require.Error(t, err, string(out))
		require.Equal(t, "Error: cannot diff schema \"\" with a database connection\n", string(out))
	})
}

func TestSpanner_DefaultsHCL(t *testing.T) {
	n := "atlas_defaults"
	stRun(t, func(t *spannerTest) {
		ddl := `
create table atlas_defaults
(
	string varchar(255) default 'hello_world',
	quoted varchar(100) default 'never say "never"',
	tBit bit(10) default b'10101',
	ts timestamp default CURRENT_TIMESTAMP,
	tstz timestamp with time zone default CURRENT_TIMESTAMP,
	number int default 42
)
`
		t.dropTables(n)
		_, err := t.db.Exec(ddl)
		require.NoError(t, err)
		realm := t.loadRealm()
		spec, err := spanner.MarshalHCL(realm.Schemas[0])
		require.NoError(t, err)
		// var s schema.Schema
		// err = spanner.EvalHCLBytes(spec, &s, nil)
		// require.NoError(t, err)
		t.dropTables(n)
		t.applyHcl(string(spec))
		ensureNoChange(t, realm.Schemas[0].Tables[0])
	})
}

func TestSpanner_Sanity(t *testing.T) {
	n := "atlas_types_sanity"
	ddl := `
DROP TYPE IF EXISTS address;
CREATE TYPE address AS (city VARCHAR(90), street VARCHAR(90));
create table atlas_types_sanity
(
    "tBit"                 bit(10)                     default b'100'                                   null,
    "tBitVar"              bit varying(10)             default b'100'                                   null,
    "tBoolean"             boolean                     default false                                not null,
    "tBool"                bool                        default false                                not null,
    "tBytea"               bytea                       default E'\\001'                             not null,
    "tCharacter"           character(10)               default 'atlas'                                  null,
    "tChar"                char(10)                    default 'atlas'                                  null,
    "tCharVar"             character varying(10)       default 'atlas'                                  null,
    "tVarChar"             varchar(10)                 default 'atlas'                                  null,
    "tText"                text                        default 'atlas'                                  null,
    "tSmallInt"            smallint                    default '10'                                     null,
    "tInteger"             integer                     default '10'                                     null,
    "tBigInt"              bigint                      default '10'                                     null,
    "tInt"                 int                         default '10'                                     null,
    "tInt2"                int2                        default '10'                                     null,
    "tInt4"                int4                        default '10'                                     null,
    "tInt8"                int8                        default '10'                                     null,
    "tCIDR"                cidr                        default '127.0.0.1'                              null,
    "tInet"                inet                        default '127.0.0.1'                              null,
    "tMACAddr"             macaddr                     default '08:00:2b:01:02:03'                      null,
    "tMACAddr8"            macaddr8                    default '08:00:2b:01:02:03:04:05'                null,
    "tCircle"              circle                      default                                          null,
    "tLine"                line                        default                                          null,
    "tLseg"                lseg                        default                                          null, 
    "tBox"                 box                         default                                          null,
    "tPath"                path                        default                                          null,
    "tPoint"               point                       default                                          null,
    "tDate"                date                        default current_date                             null,
    "tTime"                time                        default current_time                             null,
    "tTimeWTZ"             time with time zone         default current_time                             null,
    "tTimeWOTZ"            time without time zone      default current_time                             null,
    "tTimestamp"           timestamp                   default now()                                    null,
    "tTimestampTZ"         timestamptz                 default now()                                    null,
    "tTimestampWTZ"        timestamp with time zone    default now()                                    null,
    "tTimestampWOTZ"       timestamp without time zone default now()                                    null,
    "tTimestampPrec"       timestamp(4)                default now()                                    null,
    "tDouble"              double precision            default 0                                        null,
    "tReal"                real                        default 0                                        null,
    "tFloat8"              float8                      default 0                                        null,
    "tFloat4"              float4                      default 0                                        null,
    "tNumeric"             numeric                     default 0                                        null,
    "tDecimal"             decimal                     default 0                                        null,
    "tSmallSerial"         smallserial                                                                      ,
    "tSerial"              serial                                                                           ,
    "tBigSerial"           bigserial                                                                        ,
    "tSerial2"             serial2                                                                          ,
    "tSerial4"             serial4                                                                          ,
    "tSerial8"             serial8                                                                          ,
    "tArray"               text[10][10]                 default '{}'                                    null,
    "tXML"                 xml                          default '<a>foo</a>'                            null,  
    "tJSON"                json                         default '{"key":"value"}'                       null,
    "tJSONB"               jsonb                        default '{"key":"value"}'                       null,
    "tUUID"                uuid                         default  'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11' null,
    "tMoney"               money                        default  18                                     null,
    "tInterval"            interval                     default '4 hours'                               null, 
    "tUserDefined"         address                      default '("ab","cd")'                           null
);
`
	stRun(t, func(t *spannerTest) {
		t.dropTables(n)
		_, err := t.db.Exec(ddl)
		require.NoError(t, err)
		realm := t.loadRealm()
		require.Len(t, realm.Schemas, 1)
		ts, ok := realm.Schemas[0].Table(n)
		require.True(t, ok)
		expected := schema.Table{
			Name:   n,
			Schema: realm.Schemas[0],
			Columns: []*schema.Column{
				{
					Name:    "tBoolean",
					Type:    &schema.ColumnType{Type: &schema.BoolType{T: "boolean"}, Raw: "boolean", Null: false},
					Default: &schema.Literal{V: "false"},
				},
				{
					Name:    "tBool",
					Type:    &schema.ColumnType{Type: &schema.BoolType{T: "boolean"}, Raw: "boolean", Null: false},
					Default: &schema.Literal{V: "false"},
				},
				{
					Name:    "tBytea",
					Type:    &schema.ColumnType{Type: &schema.BinaryType{T: "bytea"}, Raw: "bytea", Null: false},
					Default: &schema.Literal{V: "'\\x01'"},
				},
				{
					Name:    "tCharacter",
					Type:    &schema.ColumnType{Type: &schema.StringType{T: "character", Size: 10}, Raw: "character", Null: true},
					Default: &schema.Literal{V: "'atlas'"},
				},
				{
					Name:    "tChar",
					Type:    &schema.ColumnType{Type: &schema.StringType{T: "character", Size: 10}, Raw: "character", Null: true},
					Default: &schema.Literal{V: "'atlas'"},
				},
				{
					Name:    "tCharVar",
					Type:    &schema.ColumnType{Type: &schema.StringType{T: "character varying", Size: 10}, Raw: "character varying", Null: true},
					Default: &schema.Literal{V: "'atlas'"},
				},
				{
					Name:    "tVarChar",
					Type:    &schema.ColumnType{Type: &schema.StringType{T: "character varying", Size: 10}, Raw: "character varying", Null: true},
					Default: &schema.Literal{V: "'atlas'"},
				},
				{
					Name:    "tText",
					Type:    &schema.ColumnType{Type: &schema.StringType{T: "text"}, Raw: "text", Null: true},
					Default: &schema.Literal{V: "'atlas'"},
				},
				{
					Name:    "tSmallInt",
					Type:    &schema.ColumnType{Type: &schema.IntegerType{T: "smallint"}, Raw: "smallint", Null: true},
					Default: &schema.Literal{V: "10"},
				},
				{
					Name:    "tInteger",
					Type:    &schema.ColumnType{Type: &schema.IntegerType{T: "integer"}, Raw: "integer", Null: true},
					Default: &schema.Literal{V: "10"},
				},
				{
					Name:    "tBigInt",
					Type:    &schema.ColumnType{Type: &schema.IntegerType{T: "bigint"}, Raw: "bigint", Null: true},
					Default: &schema.Literal{V: "10"},
				},
				{
					Name:    "tInt",
					Type:    &schema.ColumnType{Type: &schema.IntegerType{T: "integer"}, Raw: "integer", Null: true},
					Default: &schema.Literal{V: "10"},
				},
				{
					Name:    "tInt2",
					Type:    &schema.ColumnType{Type: &schema.IntegerType{T: "smallint"}, Raw: "smallint", Null: true},
					Default: &schema.Literal{V: "10"},
				},
				{
					Name:    "tInt4",
					Type:    &schema.ColumnType{Type: &schema.IntegerType{T: "integer"}, Raw: "integer", Null: true},
					Default: &schema.Literal{V: "10"},
				},
				{
					Name:    "tInt8",
					Type:    &schema.ColumnType{Type: &schema.IntegerType{T: "bigint"}, Raw: "bigint", Null: true},
					Default: &schema.Literal{V: "10"},
				},
				{
					Name: "tCircle",
					Type: &schema.ColumnType{Type: &schema.SpatialType{T: "circle"}, Raw: "circle", Null: true},
				},
				{
					Name: "tLine",
					Type: &schema.ColumnType{Type: &schema.SpatialType{T: "line"}, Raw: "line", Null: true},
				},
				{
					Name: "tLseg",
					Type: &schema.ColumnType{Type: &schema.SpatialType{T: "lseg"}, Raw: "lseg", Null: true},
				},
				{
					Name: "tBox",
					Type: &schema.ColumnType{Type: &schema.SpatialType{T: "box"}, Raw: "box", Null: true},
				},
				{
					Name: "tPath",
					Type: &schema.ColumnType{Type: &schema.SpatialType{T: "path"}, Raw: "path", Null: true},
				},
				{
					Name: "tPoint",
					Type: &schema.ColumnType{Type: &schema.SpatialType{T: "point"}, Raw: "point", Null: true},
				},
				{
					Name:    "tDate",
					Type:    &schema.ColumnType{Type: &schema.TimeType{T: "date"}, Raw: "date", Null: true},
					Default: &schema.RawExpr{X: "CURRENT_DATE"},
				},
				{
					Name:    "tTime",
					Type:    &schema.ColumnType{Type: &schema.TimeType{T: "time without time zone", Precision: intp(6)}, Raw: "time without time zone", Null: true},
					Default: &schema.RawExpr{X: "CURRENT_TIME"},
				},
				{
					Name:    "tTimeWTZ",
					Type:    &schema.ColumnType{Type: &schema.TimeType{T: "time with time zone", Precision: intp(6)}, Raw: "time with time zone", Null: true},
					Default: &schema.RawExpr{X: "CURRENT_TIME"},
				},
				{
					Name:    "tTimeWOTZ",
					Type:    &schema.ColumnType{Type: &schema.TimeType{T: "time without time zone", Precision: intp(6)}, Raw: "time without time zone", Null: true},
					Default: &schema.RawExpr{X: "CURRENT_TIME"},
				},
				{
					Name:    "tTimestamp",
					Type:    &schema.ColumnType{Type: &schema.TimeType{T: "timestamp without time zone", Precision: intp(6)}, Raw: "timestamp without time zone", Null: true},
					Default: &schema.RawExpr{X: "now()"},
				},
				{
					Name:    "tTimestampTZ",
					Type:    &schema.ColumnType{Type: &schema.TimeType{T: "timestamp with time zone", Precision: intp(6)}, Raw: "timestamp with time zone", Null: true},
					Default: &schema.RawExpr{X: "now()"},
				},
				{
					Name:    "tTimestampWTZ",
					Type:    &schema.ColumnType{Type: &schema.TimeType{T: "timestamp with time zone", Precision: intp(6)}, Raw: "timestamp with time zone", Null: true},
					Default: &schema.RawExpr{X: "now()"},
				},
				{
					Name:    "tTimestampWOTZ",
					Type:    &schema.ColumnType{Type: &schema.TimeType{T: "timestamp without time zone", Precision: intp(6)}, Raw: "timestamp without time zone", Null: true},
					Default: &schema.RawExpr{X: "now()"},
				},
				{
					Name:    "tTimestampPrec",
					Type:    &schema.ColumnType{Type: &schema.TimeType{T: "timestamp without time zone", Precision: intp(4)}, Raw: "timestamp without time zone", Null: true},
					Default: &schema.RawExpr{X: "now()"},
				},
				{
					Name:    "tDouble",
					Type:    &schema.ColumnType{Type: &schema.FloatType{T: "double precision", Precision: 53}, Raw: "double precision", Null: true},
					Default: &schema.Literal{V: "0"},
				},
				{
					Name:    "tReal",
					Type:    &schema.ColumnType{Type: &schema.FloatType{T: "real", Precision: 24}, Raw: "real", Null: true},
					Default: &schema.Literal{V: "0"},
				},
				{
					Name:    "tFloat8",
					Type:    &schema.ColumnType{Type: &schema.FloatType{T: "double precision", Precision: 53}, Raw: "double precision", Null: true},
					Default: &schema.Literal{V: "0"},
				},
				{
					Name:    "tFloat4",
					Type:    &schema.ColumnType{Type: &schema.FloatType{T: "real", Precision: 24}, Raw: "real", Null: true},
					Default: &schema.Literal{V: "0"},
				},
				{
					Name:    "tNumeric",
					Type:    &schema.ColumnType{Type: &schema.DecimalType{T: "numeric", Precision: 0}, Raw: "numeric", Null: true},
					Default: &schema.Literal{V: "0"},
				},
				{
					Name:    "tDecimal",
					Type:    &schema.ColumnType{Type: &schema.DecimalType{T: "numeric", Precision: 0}, Raw: "numeric", Null: true},
					Default: &schema.Literal{V: "0"},
				},
				{
					Name: "tSmallSerial",
					Type: &schema.ColumnType{Type: &schema.IntegerType{T: "smallint", Unsigned: false}, Raw: "smallint", Null: false},
					Default: &schema.RawExpr{
						X: "nextval('\"atlas_types_sanity_tSmallSerial_seq\"'::regclass)",
					},
				},
				{
					Name: "tSerial",
					Type: &schema.ColumnType{Type: &schema.IntegerType{T: "integer", Unsigned: false}, Raw: "integer", Null: false},
					Default: &schema.RawExpr{
						X: "nextval('\"atlas_types_sanity_tSerial_seq\"'::regclass)",
					},
				},
				{
					Name: "tBigSerial",
					Type: &schema.ColumnType{Type: &schema.IntegerType{T: "bigint", Unsigned: false}, Raw: "bigint", Null: false},
					Default: &schema.RawExpr{
						X: "nextval('\"atlas_types_sanity_tBigSerial_seq\"'::regclass)",
					},
				},
				{
					Name: "tSerial2",
					Type: &schema.ColumnType{Type: &schema.IntegerType{T: "smallint", Unsigned: false}, Raw: "smallint", Null: false},
					Default: &schema.RawExpr{
						X: "nextval('\"atlas_types_sanity_tSerial2_seq\"'::regclass)",
					},
				},
				{
					Name: "tSerial4",
					Type: &schema.ColumnType{Type: &schema.IntegerType{T: "integer", Unsigned: false}, Raw: "integer", Null: false},
					Default: &schema.RawExpr{
						X: "nextval('\"atlas_types_sanity_tSerial4_seq\"'::regclass)",
					},
				},
				{
					Name: "tSerial8",
					Type: &schema.ColumnType{Type: &schema.IntegerType{T: "bigint", Unsigned: false}, Raw: "bigint", Null: false},
					Default: &schema.RawExpr{
						X: "nextval('\"atlas_types_sanity_tSerial8_seq\"'::regclass)",
					},
				},
				{
					Name: "tArray",
					Type: &schema.ColumnType{Type: &spanner.ArrayType{Type: &schema.StringType{T: "text"}, T: "text[]"}, Raw: "ARRAY", Null: true},
					Default: &schema.Literal{
						V: "'{}'",
					},
				},
				{
					Name: "tJSON",
					Type: &schema.ColumnType{Type: &schema.JSONType{T: "json"}, Raw: "json", Null: true},
					Default: &schema.Literal{
						V: "'{\"key\":\"value\"}'",
					},
				},
				{
					Name: "tJSONB",
					Type: &schema.ColumnType{Type: &schema.JSONType{T: "jsonb"}, Raw: "jsonb", Null: true},
					Default: &schema.Literal{
						V: "'{\"key\": \"value\"}'",
					},
				},
			},
		}
		require.EqualValues(t, &expected, ts)
	})

	t.Run("ImplicitIndexes", func(t *testing.T) {
		stRun(t, func(t *spannerTest) {
			testImplicitIndexes(t, t.db)
		})
	})
}

func (t *spannerTest) driver() migrate.Driver {
	return t.drv
}

func (t *spannerTest) revisionsStorage() migrate.RevisionReadWriter {
	return t.rrw
}

func (t *spannerTest) applyHcl(spec string) {
	// realm := t.loadRealm()
	// var desired schema.Schema
	// err := spanner.EvalHCLBytes([]byte(spec), &desired, nil)
	// require.NoError(t, err)
	// existing := realm.Schemas[0]
	// diff, err := t.drv.SchemaDiff(existing, &desired)
	// require.NoError(t, err)
	// err = t.drv.ApplyChanges(context.Background(), diff)
	// require.NoError(t, err)
}

func (t *spannerTest) valueByVersion(values map[string]string, defaults string) string {
	if v, ok := values[t.version]; ok {
		return v
	}
	return defaults
}

func (t *spannerTest) loadRealm() *schema.Realm {
	r, err := t.drv.InspectRealm(context.Background(), &schema.InspectRealmOption{
		Schemas: []string{""},
	})
	require.NoError(t, err)
	return r
}

func (t *spannerTest) loadUsers() *schema.Table {
	return t.loadTable("users")
}

func (t *spannerTest) loadPosts() *schema.Table {
	return t.loadTable("posts")
}

func (t *spannerTest) loadTable(name string) *schema.Table {
	realm := t.loadRealm()
	require.Len(t, realm.Schemas, 1)
	table, ok := realm.Schemas[0].Table(name)
	require.True(t, ok)
	return table
}

func (t *spannerTest) users() *schema.Table {
	usersT := &schema.Table{
		Name:   "users",
		Schema: t.realm().Schemas[0],
		Columns: []*schema.Column{
			{
				Name: "id",
				Type: &schema.ColumnType{Raw: "bigint", Type: &schema.IntegerType{T: "bigint"}},
			},
			{
				Name: "x",
				Type: &schema.ColumnType{Raw: "bigint", Type: &schema.IntegerType{T: "bigint"}},
			},
		},
	}
	usersT.PrimaryKey = &schema.Index{Parts: []*schema.IndexPart{{C: usersT.Columns[0]}}}
	return usersT
}

func (t *spannerTest) posts() *schema.Table {
	usersT := t.users()
	postsT := &schema.Table{
		Name:   "posts",
		Schema: t.realm().Schemas[0],
		Columns: []*schema.Column{
			{
				Name: "id",
				Type: &schema.ColumnType{Raw: "bigint", Type: &schema.IntegerType{T: "bigint"}},
			},
			{
				Name:    "author_id",
				Type:    &schema.ColumnType{Raw: "bigint", Type: &schema.IntegerType{T: "bigint"}, Null: true},
				Default: &schema.Literal{V: "10"},
			},
			{
				Name: "ctime",
				Type: &schema.ColumnType{Raw: "timestamp", Type: &schema.TimeType{T: "timestamp"}},
				Default: &schema.RawExpr{
					X: "CURRENT_TIMESTAMP",
				},
			},
		},
		Attrs: []schema.Attr{
			&schema.Comment{Text: "posts comment"},
		},
	}
	postsT.PrimaryKey = &schema.Index{Parts: []*schema.IndexPart{{C: postsT.Columns[0]}}}
	postsT.Indexes = []*schema.Index{
		{Name: "author_id", Parts: []*schema.IndexPart{{C: postsT.Columns[1]}}},
		{Name: "id_author_id_unique", Unique: true, Parts: []*schema.IndexPart{{C: postsT.Columns[1]}, {C: postsT.Columns[0]}}},
	}
	postsT.ForeignKeys = []*schema.ForeignKey{
		{Symbol: "author_id", Table: postsT, Columns: postsT.Columns[1:2], RefTable: usersT, RefColumns: usersT.Columns[:1], OnDelete: schema.NoAction},
	}
	return postsT
}

func (t *spannerTest) url(s string) string {
	return s
}
func (t *spannerTest) realm() *schema.Realm {
	r := &schema.Realm{
		Schemas: []*schema.Schema{
			{
				Name: "",
			},
		},
	}
	r.Schemas[0].Realm = r
	return r
}

func (t *spannerTest) diff(t1, t2 *schema.Table) []schema.Change {
	changes, err := t.drv.TableDiff(t1, t2)
	require.NoError(t, err)
	return changes
}

func (t *spannerTest) migrate(changes ...schema.Change) {
	err := t.drv.ApplyChanges(context.Background(), changes)
	require.NoError(t, err)
}

func (t *spannerTest) dropTables(names ...string) {
	t.Cleanup(func() {
		_, err := t.db.Exec("DROP TABLE IF EXISTS " + strings.Join(names, ", "))
		require.NoError(t.T, err, "drop tables %q", names)
	})
}

func (t *spannerTest) dropSchemas(names ...string) {
	t.Cleanup(func() {
		_, err := t.db.Exec("DROP SCHEMA IF EXISTS " + strings.Join(names, ", ") + " CASCADE")
		require.NoError(t.T, err, "drop schema %q", names)
	})
}