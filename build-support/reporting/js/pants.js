
function toggleScope(e) {
  e.next().toggle();
  $(".visibility-icon", e).toggleClass("icon-caret-right icon-caret-down")
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
        $(targetSelector).append(data);
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

$(function(){
  tail('html/build.html', '#main-content');
});
