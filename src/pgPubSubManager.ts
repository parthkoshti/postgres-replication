// filename: src/pgPubSubManager.ts

import { Client } from "pg";

export interface PgConnectionConfig {
  host: string;
  port: number;
  user: string;
  password: string;
  database: string;
}

export interface ReplicationConfig {
  publicationName: string;
  subscriptionName: string;
  tables?: string[];
  copyData?: boolean;
  cleanTarget?: boolean; // new option
}

export class PgPubSubManager {
  private source: Client;
  private target: Client;
  private cfg: ReplicationConfig;

  constructor(
    sourceCfg: PgConnectionConfig,
    targetCfg: PgConnectionConfig,
    cfg: ReplicationConfig
  ) {
    this.source = new Client(sourceCfg);
    this.target = new Client(targetCfg);
    this.cfg = cfg;
  }

  async connect() {
    await Promise.all([this.source.connect(), this.target.connect()]);
  }

  async disconnect() {
    await Promise.all([this.source.end(), this.target.end()]);
  }

  async createPublication() {
    const { publicationName, tables } = this.cfg;
    const tablesSql = tables?.length
      ? `FOR TABLE ${tables.map((t) => `"${t}"`).join(", ")}`
      : "FOR ALL TABLES";

    const sql = `CREATE PUBLICATION "${publicationName}" ${tablesSql};`;

    try {
      await this.source.query(sql);
      console.log(`✔ Created publication "${publicationName}"`);
    } catch (e: any) {
      if (!e.message.includes("already exists")) throw e;
      console.log(`ℹ Publication "${publicationName}" already exists`);
    }
  }

  async cleanTargetTables() {
    if (!this.cfg.cleanTarget) return;

    let tables = this.cfg.tables;
    if (!tables || !tables.length) {
      // Try to derive tables from publication
      const res = await this.source.query(
        `SELECT DISTINCT t.schemaname, t.tablename
         FROM pg_publication_tables t
         WHERE pubname = $1`,
        [this.cfg.publicationName]
      );
      tables = res.rows.map((r) => `${r.schemaname}.${r.tablename}`);
    }

    if (!tables || !tables.length) {
      console.warn("⚠ No tables found to truncate on target");
      return;
    }

    console.log(`⚙ Cleaning target tables: ${tables.join(", ")}`);

    // Disable triggers temporarily to avoid FK issues during truncation
    await this.target.query("SET session_replication_role = 'replica';");

    for (const tbl of tables) {
      try {
        await this.target.query(`TRUNCATE TABLE ${tbl} CASCADE;`);
        console.log(`✔ Truncated target table ${tbl}`);
      } catch (e: any) {
        console.warn(`⚠ Skipped ${tbl}: ${e.message}`);
      }
    }

    // Restore normal trigger behavior
    await this.target.query("SET session_replication_role = 'origin';");
  }

  private quoteIdent(name: string) {
    return `"${name.replace(/"/g, '""')}"`;
  }

  async createTargetTablesIfNotExists() {
    let tables = this.cfg.tables;
    if (!tables || !tables.length) {
      const res = await this.source.query(
        `SELECT DISTINCT t.schemaname, t.tablename
         FROM pg_publication_tables t
         WHERE pubname = $1`,
        [this.cfg.publicationName]
      );
      tables = res.rows.map((r) => `${r.schemaname}.${r.tablename}`);
    }

    if (!tables || !tables.length) {
      console.warn("⚠ No tables found to create on target");
      return;
    }

    for (const full of tables) {
      const [schema, table] = full.split(".");
      if (!schema || !table) continue;

      const exists = await this.target.query(
        `SELECT 1 FROM information_schema.tables WHERE table_schema = $1 AND table_name = $2`,
        [schema, table]
      );
      if (exists.rowCount) {
        console.log(`ℹ Target table ${full} already exists`);
        continue;
      }

      // fetch columns from source
      const colsRes = await this.source.query(
        `SELECT column_name, data_type, is_nullable, column_default,
                character_maximum_length, numeric_precision, numeric_scale, udt_name
         FROM information_schema.columns
         WHERE table_schema = $1 AND table_name = $2
         ORDER BY ordinal_position`,
        [schema, table]
      );
      if (!colsRes.rowCount) {
        console.warn(`⚠ Source table ${full} not found, skipping creation`);
        continue;
      }

      const colDefs: string[] = [];
      for (const r of colsRes.rows) {
        // derive a reasonable type string
        let typeStr = "";
        if (r.data_type === "character varying") {
          typeStr = r.character_maximum_length
            ? `varchar(${r.character_maximum_length})`
            : "varchar";
        } else if (r.data_type === "character") {
          typeStr = r.character_maximum_length
            ? `char(${r.character_maximum_length})`
            : "char";
        } else if (r.data_type === "numeric") {
          typeStr =
            r.numeric_precision != null && r.numeric_scale != null
              ? `numeric(${r.numeric_precision},${r.numeric_scale})`
              : "numeric";
        } else if (r.udt_name && r.udt_name.startsWith("_")) {
          // array type
          typeStr = `${r.udt_name.substring(1)}[]`;
        } else {
          typeStr = r.udt_name || r.data_type;
        }

        const nullable = r.is_nullable === "NO" ? "NOT NULL" : "";
        const def = r.column_default ? `DEFAULT ${r.column_default}` : "";
        colDefs.push(
          `${this.quoteIdent(
            r.column_name
          )} ${typeStr} ${def} ${nullable}`.trim()
        );
      }

      // fetch primary key if present
      const pkRes = await this.source.query(
        `SELECT kcu.column_name
         FROM information_schema.table_constraints tc
         JOIN information_schema.key_column_usage kcu
           ON tc.constraint_name = kcu.constraint_name
          AND tc.table_schema = kcu.table_schema
         WHERE tc.constraint_type = 'PRIMARY KEY'
           AND tc.table_schema = $1
           AND tc.table_name = $2
         ORDER BY kcu.ordinal_position`,
        [schema, table]
      );
      if (pkRes.rowCount) {
        const pkCols = pkRes.rows
          .map((r) => this.quoteIdent(r.column_name))
          .join(", ");
        colDefs.push(`PRIMARY KEY (${pkCols})`);
      }

      const createSql = `CREATE TABLE IF NOT EXISTS ${this.quoteIdent(
        schema
      )}.${this.quoteIdent(table)} (\n  ${colDefs.join(",\n  ")}\n);`;

      try {
        await this.target.query(createSql);
        console.log(`✔ Created target table ${full}`);
      } catch (e: any) {
        console.warn(`⚠ Failed to create ${full}: ${e.message}`);
      }
    }
  }

  async createSubscription() {
    const { subscriptionName, publicationName, copyData } = this.cfg;
    const exists = await this.target.query(
      "SELECT 1 FROM pg_subscription WHERE subname = $1",
      [subscriptionName]
    );

    if (exists.rowCount) {
      console.log(`ℹ Subscription "${subscriptionName}" already exists`);
      return;
    }

    const connStr = `host=${this.source.host} port=${this.source.port} dbname=${this.source.database} user=${this.source.user} password=${this.source.password}`;
    const withClause = copyData === false ? "WITH (copy_data = false)" : "";
    const sql = `CREATE SUBSCRIPTION "${subscriptionName}" CONNECTION '${connStr}' PUBLICATION "${publicationName}" ${withClause};`;

    await this.target.query(sql);
    console.log(`✔ Created subscription "${subscriptionName}"`);
  }

  /**
   * Monitor replication progress and print updates every second.
   * If durationSeconds > 0 the monitor runs for that many seconds, otherwise runs indefinitely.
   */
  async monitorReplicationProgress(durationSeconds = 0): Promise<void> {
    const subName = this.cfg.subscriptionName;
    const start = Date.now();

    const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));

    while (true) {
      try {
        const subRes = await this.target.query(
          `SELECT subname,
                  MAX(received_lsn) AS received_lsn,
                  MAX(last_msg_send_time) AS last_msg_send_time,
                  MAX(last_msg_receipt_time) AS last_msg_receipt_time
           FROM pg_stat_subscription
           WHERE subname = $1
           GROUP BY subname`,
          [subName]
        );

        if (!subRes.rowCount) {
          console.log(`[monitor] No stats for subscription "${subName}" yet`);
        } else {
          const row = subRes.rows[0];
          const receivedLsn = row.received_lsn;
          const lastReceipt =
            row.last_msg_receipt_time || row.last_msg_send_time;
          const lagSeconds =
            lastReceipt != null
              ? (Date.now() - new Date(lastReceipt).getTime()) / 1000
              : null;

          let currentLsn: string | null = null;
          let bytesBehind: string | null = null;
          if (receivedLsn) {
            const diffRes = await this.source.query(
              `SELECT pg_current_wal_lsn() AS current_lsn,
                      pg_wal_lsn_diff(pg_current_wal_lsn(), $1::pg_lsn) AS bytes_behind`,
              [receivedLsn]
            );
            if (diffRes.rowCount) {
              currentLsn = diffRes.rows[0].current_lsn;
              bytesBehind = diffRes.rows[0].bytes_behind;
            }
          }

          console.log(
            `[monitor] ${new Date().toISOString()} sub="${subName}" received_lsn=${
              receivedLsn || "null"
            } current_lsn=${currentLsn || "null"} bytes_behind=${
              bytesBehind || "null"
            } lag_s=${lagSeconds != null ? lagSeconds.toFixed(1) : "null"}`
          );
        }
      } catch (e: any) {
        console.warn(
          `[monitor] error fetching replication stats: ${e.message || e}`
        );
      }

      if (durationSeconds > 0 && (Date.now() - start) / 1000 >= durationSeconds)
        break;
      await sleep(1000);
    }
  }

  async setupFullPipeline() {
    await this.connect();

    await this.createPublication();

    // ensure tables exist on target before truncating / subscribing
    await this.createTargetTablesIfNotExists();

    if (this.cfg.cleanTarget) {
      await this.cleanTargetTables();
    }

    await this.createSubscription();
    console.log(
      "✔ Replication setup complete — starting monitor (updates every 1s)"
    );

    // start monitoring in background (runs indefinitely by default)
    this.monitorReplicationProgress().catch(async (err) => {
      console.error("Replication monitor error:", err);
      try {
        await this.disconnect();
      } catch (_) {}
    });
  }
}
