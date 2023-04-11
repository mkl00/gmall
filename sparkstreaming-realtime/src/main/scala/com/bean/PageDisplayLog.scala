package com.bean

//页面曝光日志 Bean
case class PageDisplayLog(
                           mid: String,
                           user_id: String,
                           province_id: String,
                           channel: String,
                           is_new: String,
                           model: String,
                           operate_system: String,
                           version_code: String,
                           page_id: String,
                           last_page_id: String,
                           page_item: String,
                           page_item_type: String,
                           during_time: Long,
                           display_type: String,
                           display_item: String,
                           display_item_type: String,
                           display_order: Long,
                           display_pos_id: Long,
                           ts: Long
                         )

