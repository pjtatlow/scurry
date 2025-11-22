![logo](./scurry.png)

# scurry
A new kind of CockroachDB schema management tool.
___

## What is it?

scurry lets you define your schema as a series of `CREATE` statements that represent the end state you want. It then provides the tools to:

1. Update your local database
2. Generate migrations for your production database
3. Run an in-memory database with your schema

## Why use scurry?

Other schema management tools let you define your schema in either:

1. Your application programming language, or a DSL
2. A series of SQL migrations

The issue with the first approach is that none of these languages have support for every possible CockroachDB setting / configuration. There might be escape hatches to let you set up a virual computed column, or an index with a `STORED` clause, but it's at least a pain and at worst impossible.

The issue with the second is that applying SQL migrations in CockroachDB [can be extremely slow](https://github.com/cockroachdb/cockroach/issues/106301). So when you want to create your a mock database by running all of the migrations, it can take minutes even with a relatively small number of changes.


## How do I use it?

Guides coming soon...
