### Distribute Database Test Framework

#### Setup

The binary should be downloaded manually 
and put it into `test/bin` directory like
`test/bin/tidb-server`

After that, run `go test`

#### What I did

- This framework can start a cluster including
a TiDB, 3 TiKV and 1 PD by default. And they can
be config very easily in the code

- This framework can test simple DML(insert, update,
delete,select), and DDL(create table, drop table).

- This framework running DML concurrently, and verify itself

- This framework provide restart tidb server randomly
(but it is not stable now, some trick thing will happend and cause test failed)

- Actually, this framework can provide automatically download
binary, but I think that's not necessary in most cases.

#### What I should do

- Should provide more stable restart tidb server(chaos)
 
- Should build binary from source code by analysing the commit hash num (CI)

- Should have more test about DDL and other conner test case

#### Thank you