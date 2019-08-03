import scala.util.Try

package object objektwerks {
  def createSparkEventsDir(dir: String): Boolean = {
    import java.nio.file.{Files, Paths}
    val path = Paths.get(dir)
    if (!Files.exists(path))
      Try ( Files.createDirectories(path) ).isSuccess
    else true
  }
}