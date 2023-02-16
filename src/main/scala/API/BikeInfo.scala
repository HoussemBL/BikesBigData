package API

import java.util.List

case class BikeInfo(date_stolen: Long, description: String, frame_colors: List[String], frame_model: String,
                    id: Long, is_stock_img: Boolean, large_img: String, location_found: Boolean,
                    manufacturer_name: String, external_id: Long, registry_name: String,
                    registry_url: String, serial: String, status: String, stolen: Boolean,
                    stolen_coordinates: List[Float], stolen_location: String, thumb: String,
                    title: String, url: String, year: Int
                   )


case class BikeArray(bikes: java.util.List[BikeInfo])