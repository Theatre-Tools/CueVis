
const cue_element = $('#cue');
const active = new EventSource('/api/active/stream');
active.onopen = () => {
    console.info('Connection to server has opened.');
    cue_element.text('');
};

active.addEventListener('active', function (cue) {
    console.warn(cue);
    let active_info = JSON.parse(cue.data);
    console.info('Received active cue update:', active_info);
    cue_element.text(`LX ${active_info.active}`);
});





$.ajax({
    url: '/api/status',
    method: 'GET',
    success: function (response) {
        console.info('Server status:', response['status']['response']);
        $('#no-connection-error').hide();
    },
    error: function (xhr, status, error) {
        console.error('Failed to retrieve server status:', error, status);
        $('#no-connection-error').show();
    }
})