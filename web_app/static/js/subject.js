
$( document ).ready(function() {
	$("#search").on("keyup", function() {
		var value = $(this).val().toLowerCase();
		$("#table_data tr").filter(function() {
			$(this).toggle($(this).text().toLowerCase().indexOf(value) > -1)
		});
	});
});

// this.update_record = function(type, id){
//     var html_id = 'update_create_' + id;
//     var dialog = $("#" + html_id);

//     if(dialog.length !== ZERO){
//         if(!dialog.dialog("isOpen")){
//             dialog.dialog("open");
//         }
//     }

//     else{
//         // var record_info = get_record_info(type, id)

//         // if(record_info !== null){
//         //     var required_fields = get_required_fields(type, id);
//             var html = set_create_update_html(record_info, type, id, required_fields);

//             var title = 'Update';

//             $(function() {
//                 $(html).dialog({
//                     title: title,
//                     width:'auto',
//                     height:'auto',
//                     create: function(){
//                         if(type === 'job_queue'){
//                             add_executable_lookahead(id, type);
//                             add_enqueued_object_class_lookahead(id, type);
//                         }
//                     },
//                     buttons: {
//                       Save: function() {
//                         var valid = validate_save(id, type);
//                         if(valid){
//                             var success = save_update(id, type);
//                             if(success){
//                                 $(this).dialog("close");
//                                 location.reload(); 
//                             }
//                         }
//                       }
//                     }
//                 });
//             } );     

            
//         // }
//     }
// }

//     function set_create_update_html(record_info, type, id, required_fields){
//         var html = '<div id="update_create_' + id + '"><div class= "update_create_error_messages" id="update_create_error_messages_'+ id + '"></div>';
//         html+= '<table>';

//         var order_length = record_info.order_length;

//         for(var index = 0; index < order_length; index++){
//             var items = record_info[index];

//             for(var key in items){
//                 html+='<tr>';
                
//                 var value = null_to_empty(items[key]);

//                 var required = '';
//                 if(key in required_fields){
//                     required = '*';
//                 }

//                 html+='<th>' + key + required + '</th>'; 

//                 html+='<th><input id="' + get_create_update_id(id, type, key) + '" class="save_update save_update_'+ id + '" type="text" name="' + key + '" value="' + value + '"></th>';
//                 html+='</tr>';
//             }
//         }

//         html+= '</table></div>';

//         return html;
//     }