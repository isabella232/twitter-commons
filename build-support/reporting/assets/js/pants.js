
pants = {
  toggleScope: function(id) {
    $("#" + id + "-content").toggle();
    $("#" + id + "-icon").toggleClass("icon-caret-right icon-caret-down")
  },

  expandScope: function(id) {
    $("#" + id + "-content").show();
    $("#" + id + "-icon").removeClass("icon-caret-right").addClass("icon-caret-down")
  },

  collapseScope: function(id) {
    $("#" + id + "-content").hide();
    $("#" + id + "-icon").removeClass("icon-caret-down").addClass("icon-caret-right")
  },

  append: function(fromSelector, toSelector) {
    $(fromSelector).appendTo($(toSelector)).show();
  },

  // Creates an object that knows how to manage multiple timers, and periodically emit them.
  createTimerManager: function() {
    // The start time (in ms since the epoch) of each timer.
    // We emit each timer to the element(s) selected by the appropriate selector.
    // id -> {startTime: ..., selector: ...}
    var timers = {};

    // A handle to the polling event, so we can cancel it if needed.
    var timingEvent = undefined;

    function updateTimers() {
      var now = $.now();
      var secs = undefined;
      var timeStr = undefined;
      var i = undefined;
      $.each(timers, function(id, timer) {
        //secs = '' + (now - timer.startTime) / 1000 + '000';
        //i = secs.indexOf('.');
        //timeStr = ((i == -1) ? secs + '.000' : secs.substr(0, i + 4)) + 's';
        $(timer.selector).html('' + Math.round((now - timer.startTime) / 1000 - 0.5) + 's');
      });
    }

    return {
      startTimer: function(id, selector, init) {
        timers[id] = { 'startTime': init ? init : $.now(), 'selector': selector };
        if (!timingEvent) {
          timingEvent = window.setInterval(updateTimers, 1000);
        }
      },

      stopTimer: function(id) {
        delete timers[id];
        var numTimers = 0;
        $.each(timers, function(k,v) { numTimers++ });
        if (numTimers == 0) {
          window.clearInterval(timingEvent);
          timingEvent = undefined;
        }
      }
    }
  },

  // Creates an object that knows how to tail multiple files by periodically polling the server.
  // Each polled file is associated with an id, so we can multiplex multiple tailings on
  // on a single server request.
  createTailer: function() {
    // State of each file we're polling (its path relative to the root, and our current position in it).
    // id -> {path: ..., pos: ... }
    var tailedFileStates = {};

    // When we get new data from a file, we append it to the element(s) selected by the appropriate selector.
    // id -> selector.
    var tailedFileTargetSelectors = {};

    // ids of files we're done with.
    // id -> true.
    var toBeStopped = {};

    // ids of files that have been tailed at least once.
    // id -> true.
    var hasBeenTailed = {};

    // We use these to ensure that we tail each requested file at least once. Otherwise a stopTailing
    // could happen before any data has been requested.

    // A handle to the polling event, so we can cancel it if needed.
    var tailingEvent = undefined;

    function pollOnce() {
      function forgetId(id) {
        delete tailedFileStates[id];
        delete tailedFileTargetSelectors[id];
        delete toBeStopped[id];
        delete hasBeenTailed[id];

        var n = 0;
        $.each(tailedFileStates, function(k, v) { n += 1; });
        if (!n) {
          window.clearInterval(tailingEvent);
          tailingEvent = undefined;
        }
      }

      $.ajax({
        url: '/tail',
        type: 'GET',
        data: { q: JSON.stringify(tailedFileStates) },
        dataType: 'json',
        success: function(data, textStatus, jqXHR) {
          function appendNewData() {
            $.each(data, function(id, val) {
              if (id in tailedFileTargetSelectors) {
                $(tailedFileTargetSelectors[id]).append(val);
              }
              if (id in tailedFileStates) {
                tailedFileStates[id].pos += val.length;
              }
              hasBeenTailed[id] = true;
            });
          }
          function checkForStopped() {
            for (var id in toBeStopped) {
              if (toBeStopped.hasOwnProperty(id) && id in hasBeenTailed) {
                // This tailing is no longer needed.
                forgetId(id);
              }
            }
          }
          appendNewData();
          checkForStopped();
        },
        error: function(data, textStatus, jqXHR) {
          // TODO: Something?
        }
      });
    }

    return {
      // Call this to start tailing the specified file, appending its content to the element(s)
      // selected by the selector. You must assign some unique id to the request.
      startTailing: function(id, path, targetSelector) {
        tailedFileStates[id] = {
          'path': path,
          'pos': 0
        };
        tailedFileTargetSelectors[id] = targetSelector;
        if (!tailingEvent) {
          tailingEvent = window.setInterval(pollOnce, 200);
        }
      },

      // Stop the specified tailing.
      stopTailing: function(id) {
        toBeStopped[id] = true;
      }
    }
  }
};

// We really only need one global one of each of these. So here they are.
pants.timerManager = pants.createTimerManager();
pants.tailer = pants.createTailer();
