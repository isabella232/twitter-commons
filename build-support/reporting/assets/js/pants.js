
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


function tail(path, targetSelector) {
  var pos = 0;
  var interval = window.setInterval(poll, 200);

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
        // TODO: Something.
      }
    });
  }
}
