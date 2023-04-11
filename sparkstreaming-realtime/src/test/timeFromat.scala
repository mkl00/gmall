import java.text.SimpleDateFormat
import java.util.Date

import javax.xml.crypto.Data

object timeFromat {
  def main(args: Array[String]): Unit = {
    val sdf: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    val str: String = sdf.format(new Date("1646456991000".toLong))



    print(str)
  }
}
