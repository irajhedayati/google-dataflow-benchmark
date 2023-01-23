package ca.dataedu.dataflow.benchmark

import scala.sys.process._

object GCloud {

  case class CommandResult(stdout: String, stderr: String, status: Int)

  /** It will add --project, --region, and --format=json argument
    *
    * @param config
    *   to get the path to `gcloud` binary as well as project and region
    * @param command
    *   the command to run
    * @return
    *   standard output and error as well the status code
    */
  def runGcloud(config: Config, command: String): CommandResult = {
    val commandToRun =
      s"${config.gcloudPath} $command --project=${config.project} --region=${config.region} --format=json"
    val stdout = new StringBuilder
    val stderr = new StringBuilder
    val status = commandToRun ! ProcessLogger(stdout append _, stderr append _)
    CommandResult(stdout.toString(), stderr.toString(), status)
  }

}
