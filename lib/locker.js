var delscriptSHA;
/*jshint laxbreak:true*/
var delScript =  'if redis.call("get",KEYS[1]) == ARGV[1] '
                +'then '
                +'  return redis.call("del",KEYS[1]) '
                +'else '
                +'  return 0 '
                +'end';

//Lock Queue is a queue of LockEntries that are waiting for the availability
//of a lock in order to execute
function LockQueue(lockName, lockToken, client, subclient) {
    this.client = client;
    this.subclient = subclient;
    this.queue = [];
    this.shortLockName = lockName;
    this.lockName = "lock:"+lockName;
}

//Add a lock entry to the queue
//If this is the first one, subscribe to the
//appropriate lock channel
LockQueue.prototype.enqueue = function enqueue(acquireTimeout, holdTimeout, callback) {
    var self = this;
    var cb = callback;
    var lockEntry = {
        pending_timeout: setTimeout(function() {
            lockEntry.run(new Error("Timeout waiting for lock "+self.shortLockName), function() {});
            cb = function(err, releaser) { releaser(); };
        }, acquireTimeout),
        hold_timeout: holdTimeout,
        run: function(err, releaser) {
            clearTimeout(lockEntry.pending_timeout);
            cb(err, releaser);
        }
    };
    self.queue.push(lockEntry);
    self.pump();
};

//Try to acquire the lock
//If we fail, we'll wait for an event before trying again
//Right now, that's a publication, or another item being added
//We might want to also add a periodic heartbeat
LockQueue.prototype.tryAcquireLock = function tryAcquireLock(acquisition_callback) {
    var self = this;
    if(self.queue.length === 0) {
        return;
    }
    var lockTimeoutValue = self.queue[0].hold_timeout;
    if(lockTimeoutValue === undefined) return acquisition_callback(new Error("Invaid lock hold timeout"));
    this.client.set(self.lockName, self.lockToken, 'NX', 'EX', lockTimeoutValue/1000, function(err, result) {
        return acquisition_callback(err, result === "OK");
    });
};

//If there is something in the queue, check if we can get the lock and
//run the next item
LockQueue.prototype.pump = function pump() {
    var self = this;
    if(!delscriptSHA) {
        needsPump.push(this);
    }
    if(self.queue.length === 0) return;
    self.tryAcquireLock(function acquireCallback(err, acquired) {
        if(self.queue.length === 0) return;
        if(err || acquired) {
            var entry = self.queue.shift();
            entry.run(err, function() {
                self.client.evalsha(delscriptSHA, 1, self.lockName, self.lockToken, function () {});
                self.client.publish('lock_channel', self.shortLockName, function() {});
            });
        }
    });
};

var needsPump = [];
function LockManager(lockToken, redis, subredis, opts) {
    var namedQueues = {};
    subredis.subscribe('lock_channel');
    subredis.on('message', function(channel, lockName) {
        var queue = namedQueues[lockName];
        if(queue) queue.pump();
    });
    redis.script('load', delScript, function(err, result) {
        delscriptSHA = result;
        for(var i = 0; i < needsPump.length; i++) {
            needsPump[i].pump();
        }
        needsPump = [];
    });
    this.logger = opts.logger || {};
    this.get = function(lockName) {
        var queue = namedQueues[lockName];
        if(!queue) {
            queue = new LockQueue(lockToken, lockName, redis, subredis);
            namedQueues[lockName] = queue;
        }
        return queue;
    };

    function acquireLock(lockName, acquireTimeout, holdTimeout, callback) {
        var queue = LockQueue.get(lockName);
        queue.enqueue(acquireTimeout, holdTimeout, callback);
    }

    //Macke the locker function more convenient
    this.lockCB = function(name, original_callback, wait, timeout, task_fn) {
        acquireLock(name, wait, timeout, function(error, release) {
            task_fn(error, function unlocking_callback() {
                original_callback.apply(global, arguments);
                release();
            });
        });
    };

    //Macke the locker function more convenient
    this.locked = function(name, wait, timeout, task_fn) {
        acquireLock(name, wait, timeout, function(error, release) {
            task_fn(error, release);
        });
    };
}

exports = LockManager;
