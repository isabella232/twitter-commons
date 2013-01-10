
function toggleScope(e) {
  $(".scope-content", e).toggle();
  $(".visibility-icon", e).toggleClass("icon-caret-right icon-caret-down")
}

function expandScope(e) {
  $(".scope-content", e).show();
  $(".visibility-icon", e).removeClass("icon-caret-right").addClass("icon-caret-down")
}

function collapseScope(e) {
  $(".scope-content", e).hide();
  $(".visibility-icon", e).removeClass("icon-caret-down").addClass("icon-caret-right")
}


function tail(path, targetSelector) {
  var pos = 0;
  var interval = window.setInterval(poll, 100);

  function poll() {
    $.ajax({
      url: path,
      data: {'s': pos},
      dataType: 'text',
      success: function(data, textStatus, jqXHR) {
        pos += data.length;
        var isScrolledAllTheWayDown = (window.document.scrollTop == window.document.scrollHeight);
        $(targetSelector).append(data);
        if (isScrolledAllTheWayDown) {
          // If the reader has the scrollbar all the way down, she probably wants to scroll down
          // further automatically when we have new data to add.
          window.document.scrollTop = window.document.scrollHeight;
        }
      },
      error: function(data, textStatus, jqXHR) {
        // Probably because pants has finished running, so there's no server.
        // Load the content directly from the file, and stop polling.
        // This also allows us to open old reports as file://.
        window.clearInterval(interval);
        $(targetSelector).attr('src', path);
      }
    });
  }
}
