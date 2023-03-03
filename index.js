const Writable = require('stream').Writable
const pg = require('pg')
const _ = require('lodash');

class LogStream extends Writable {
  constructor (options) {
    super(options)

    if (options.connection === undefined || options.tableName === undefined) {
      throw new Error('Invalid bunyan-postgres stream configuration')
    }

    if (options.connection.client && options.connection.client.makeKnex) {
      this.knex = options.connection
      this._write = this._writeKnex
    }

    if (typeof options.connection === 'object') {
      this.connection = options.connection
      this._write = this._writePgPool
      this.pool = new pg.Pool(this.connection)
      this.on('finish', () => {
        if (this.pool) {
          return this.pool.end()
        }
      })
    }

    this.tableName = options.tableName
    this.schema = options.schema
  }

  _writeKnex (chunk, env, cb) {
    const content = JSON.parse(chunk.toString())
    this.knex
      .insert(this._logColumns(content))
      .into(this.tableName)
      .asCallback(cb)
  }

  _logColumns (content) {
    if (this.schema) {
      const schemaObject = {}
      const columnName = Object.keys(this.schema)
      columnName.forEach(column => {
        if (_.get(content, this.schema[column], null)) {
          schemaObject[column] = _.get(content, this.schema[column], null)
        }
      })
      return schemaObject
    } else {
      return {
        name: content.name,
        level: content.level,
        hostname: content.hostname,
        msg: content.msg,
        pid: content.pid,
        time: content.time,
        content: JSON.stringify(content).replace(/\'\'?/g, `''`)
      }
    }
  }

  _generateRawQuery (content) {
    let query = `insert into ${this.tableName} (`;
    const columnNames = Object.keys(this.schema);
    columnNames.forEach(column => {
      if(_.get(content,this.schema[column], null)){
        query = `${query}${column},`
      }
    });
    query = `${query.slice(0, -1)}) values (`;
    columnNames.forEach(column => {
      let data = _.get(content,this.schema[column], null);
      if(data){
        if(typeof data === 'object'){
          data = JSON.stringify(data).replace(/\'\'?/g, `''`);
        }
        query = `${query} '${ data }',`
      }
    });
    query = `${query.slice(0, -1)} )`;

    return query;
  }

  writePgPool (client, content) {
    if(this.schema){
      return client.query(
        this._generateRawQuery(content))
    }
    return client.query({
      text: `insert into ${
        this.tableName
      } (name, level, hostname, msg, pid, time, content) values ($1, $2, $3, $4, $5, $6, $7);`,
      values: [
        content.name,
        content.level,
        content.hostname,
        content.msg,
        content.pid,
        content.time,
        JSON.stringify(content)
          .split("'")
          .join("''")
      ]
    })
  }

  _writePgPool (chunk, env, cb) {
    const content = JSON.parse(chunk.toString())
    this.pool
      .connect()
      .then(client => {
        return this.writePgPool(client, content).then(result => {
          cb(null, result.rows)
          client.release()
        })
      })
      .catch(err => cb(err))
  }
}

module.exports = (options = {}) => {
  return new LogStream(options)
}
