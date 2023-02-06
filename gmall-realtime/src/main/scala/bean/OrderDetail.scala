package bean

case class OrderDetail(id: String,
                       order_id: String,
                       sku_id: String,
                       sku_name: String,
                       img_url: String,
                       order_price: String,
                       sku_num: String
                      )
