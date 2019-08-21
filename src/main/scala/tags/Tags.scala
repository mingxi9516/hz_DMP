package tags

trait Tags {
  /**
    * 标签接口
    */
  def getTatgs(args:Any*):List[(String,Int)]
}
