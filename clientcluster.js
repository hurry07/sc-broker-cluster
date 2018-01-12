var async = require('async');
var EventEmitter = require('events').EventEmitter;

/**
 * 把任务啥映射到不同 client process
 * @param {*[]} clients sc-broker.Client
 * @constructor
 */
var ClientCluster = function (clients) {
  var self = this;

  /**
   * 收集消息然后在当前对象上重新抛出
   */
  var handleMessage = function () {
    var args = Array.prototype.slice.call(arguments);
    self.emit.apply(self, ['message'].concat(args));
  };

  /**
   * 接受所有来自 broker.js 的消息
   */
  clients.forEach(function (client) {
    client.on('message', handleMessage);
  });

  var i, method;
  var client = clients[0];
  var clientIds = []; // clients 序号数组

  /**
   * sc-broker.Client 对外提供的方法
   * @type {string[]}
   */
  var clientInterface = [
    'subscribe',
    'isSubscribed',
    'unsubscribe',
    'publish',
    'set',
    'getExpiry',
    'add',
    'concat',
    'get',
    'getRange',
    'getAll',
    'count',
    'registerDeathQuery',
    'exec',
    'query',
    'remove',
    'removeRange',
    'removeAll',
    'splice',
    'pop',
    'hasKey',
    'send',
    'end'
  ];

  var clientUtils = [
    'extractKeys',
    'extractValues'
  ];

  /**
   * 监听 error & warning 事件
   */
  for (var i in clients) {
    if (clients.hasOwnProperty(i)) {
      var client = clients[i];

      // 多个 process error & warning 消息转发到当前对象(EventEmitter)
      client.on('error', function (error) {
        self.emit('error', error);
      });
      client.on('warning', function (warning) {
        self.emit('warning', warning);
      });
      client.id = i;     // 序号
      clientIds.push(i); // 序号数组
    }
  }

  // Default mapper maps to all clients.
  var mapper = function () {
    return clientIds;
  };

  /**
   * 在当前对象上绑定代理到其他线程的函数
   */
  clientInterface.forEach(function (method) {
    self[method] = function () {
      var key = arguments[0];
      var lastArg = arguments[arguments.length - 1];
      var results = [];
      var mapOutput = self.detailedMap(key, method);
      var activeClients = mapOutput.targets;

      if (lastArg instanceof Function) {
        if (mapOutput.type === 'single') {
          activeClients[0][method].apply(activeClients[0], arguments); // 请求对应线程上的方法

        } else {
          var result;
          var tasks = [];
          var args = Array.prototype.slice.call(arguments, 0, -1); // 调用当前方法的参数, 最后一个参数是 callback
          var cb = lastArg;
          var len = activeClients.length;

          for (var i = 0; i < len; i++) {
            /**
             * 当前方法的目标 client
             */
            (function (activeClient) {
              tasks.push(function () {
                var callback = arguments[arguments.length - 1];
                result = activeClient[method].apply(activeClient, args.concat(callback));
                results.push(result);
              });
            })(activeClients[i]);
          }

          async.parallel(tasks, cb);
        }

      } else {
        var len = activeClients.length;

        for (var i = 0; i < len; i++) {
          result = activeClients[i][method].apply(activeClients[i], arguments);
          results.push(result);
        }
      }

      return results;
    }
  });

  var multiKeyClientInterface = [
    'expire',
    'unexpire'
  ];

  multiKeyClientInterface.forEach(function (method) {
    self[method] = function () {
      var activeClients, activeClientsLen, mapping, key;
      var keys = arguments[0];
      var tasks = [];
      var results = [];
      var expiryMap = {};

      var cb = arguments[arguments.length - 1];
      var len = keys.length;

      for (var j = 0; j < len; j++) {
        key = keys[j];
        activeClients = self.map(key, method);
        activeClientsLen = activeClients.length;
        for (var k = 0; k < activeClientsLen; k++) {
          mapping = activeClients[k].id;
          if (expiryMap[mapping] == null) {
            expiryMap[mapping] = [];
          }
          expiryMap[mapping].push(key);
        }
      }

      var partArgs = Array.prototype.slice.call(arguments, 1, -1);

      for (mapping in expiryMap) {
        if (expiryMap.hasOwnProperty(mapping)) {
          (function (activeClient, expiryKeys) {
            var newArgs = [expiryKeys].concat(partArgs);
            tasks.push(function () {
              var callback = arguments[arguments.length - 1];
              var result = activeClient[method].apply(activeClient, newArgs.concat(callback));
              results.push(result);
            });
          })(clients[mapping], expiryMap[mapping]);
        }
      }
      async.parallel(tasks, cb);

      return results;
    };
  });

  clientUtils.forEach(function (method) {
    this[method] = client[method].bind(client);
  });

  this.setMapper = function (mapperFunction) {
    mapper = mapperFunction;
  };

  this.getMapper = function (mapperFunction) {
    return mapper;
  };

  /**
   * @param {*}      key    可能是 channel 的名字等等
   * @param {string} method 当前要完成的操作名
   * @return {{type: *, targets: *}} 返回映射的 clientId 数组
   */
  this.detailedMap = function (key, method) {
    var result = mapper(key, method, clientIds);
    var targets, type;

    if (typeof result === 'number') {
      type = 'single';
      targets = [clients[result % clients.length]];
    } else {
      type = 'multi';
      if (result instanceof Array) {
        var dataClients = [];
        for (var i in result) {
          if (result.hasOwnProperty(i)) {
            dataClients.push(clients[result[i] % clients.length]);
          }
        }
        targets = dataClients;
      } else {
        targets = [];
      }
    }

    return {type: type, targets: targets};
  };

  this.map = function (key, method) {
    return self.detailedMap(key, method).targets;
  };
};

ClientCluster.prototype = Object.create(EventEmitter.prototype);

module.exports.ClientCluster = ClientCluster;
