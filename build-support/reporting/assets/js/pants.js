
function toggleScope(id) {
  $("#" + id + "-content").toggle();
  $("#" + id + "-icon").toggleClass("icon-caret-right icon-caret-down")
}
function expandScope(id) {
  $("#" + id + "-content").show();
  $("#" + id + "-icon").removeClass("icon-caret-right").addClass("icon-caret-down")
}

function collapseScope(id) {
  $("#" + id + "-content").hide();
  $("#" + id + "-icon").removeClass("icon-caret-down").addClass("icon-caret-right")
}

// id -> {path: ..., pos: ... }
tailedFileStates = {};

// id -> selector
tailedFileTargetSelectors = {};

// id -> true. We use this to make sure that everything is tailed at least once.
toBeStopped = {};
hasBeenTailed = {};

var tailingId = undefined;

function startTailing(id, path, targetSelector) {
  tailedFileStates[id] = {
    'path': path,
    'pos': 0
  };
  tailedFileTargetSelectors[id] = targetSelector;
  if (!tailingId) {
    tailingId = window.setInterval(poll, 200);
  }
}

function stopTailing(id) {
  toBeStopped[id] = true;
}

function forget(id) {
  delete tailedFileStates[id];
  delete tailedFileTargetSelectors[id];
  delete toBeStopped[id];
  delete hasBeenTailed[id];

  var n = 0;
  $.each(tailedFileStates, function(k, v) { n += 1; });
  if (!n) {
    window.clearInterval(tailingId);
    tailingId = undefined;
  }
}

function poll() {
  $.ajax({
    url: '/tail',
    type: 'GET',
    data: { q: JSON.stringify(tailedFileStates) },
    dataType: 'json',
    success: function(data, textStatus, jqXHR) {
      for (var id in data) {
        if (data.hasOwnProperty(id)) {
          if (id in tailedFileTargetSelectors) {
            $(tailedFileTargetSelectors[id]).append(data[id]);
          }
          if (id in tailedFileStates) {
            tailedFileStates[id].pos += data[id].length;
          }
          hasBeenTailed[id] = true;
        }
      }
      for (var idToBeStopped in toBeStopped) {
        if (toBeStopped.hasOwnProperty(idToBeStopped) && idToBeStopped in hasBeenTailed) {
          forget(idToBeStopped);
        }
      }
    },
    error: function(data, textStatus, jqXHR) {
      // TODO: Something.
    }
  });
}
