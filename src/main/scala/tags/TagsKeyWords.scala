package tags

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.Row

object TagsKeyWords extends Tags {
  /**
    * 关键字标签
    */
  override def getTatgs(args: Any*): List[(String, Int)] = {
    var list = List[(String,Int)]()
    //获取参数
    val data = args(0).asInstanceOf[Row]
    val keyWords = args(1).asInstanceOf[Broadcast[Array[String]]]

    //获取关键字
    val keyWord = data.getAs[String]("keywords")

    val filterKeyWOrd: Array[String] = keyWord.split("\\|").filter(key => {
      key.length >= 3 && key.length <= 8 && !keyWords.value.contains(key)
    })

    filterKeyWOrd.filter(!_.isEmpty).foreach(word=>{
      list:+=("K"+word,1)
    })
    list.distinct
  }
}
