package pipelines.images.cifar

import breeze.linalg.DenseVector
import nodes.CifarLoader
import nodes.images.{ImageVectorizer, LabelExtractor, ImageExtractor, GrayScaler}
import nodes.learning.LinearMapEstimator
import nodes.util.{ClassLabelIndicatorsFromIntLabels, Cacher}
import org.apache.spark.{SparkContext, SparkConf}
import pipelines.Logging
import utils.Stats
import scopt.OptionParser


object LinearPixels extends Logging {
  val appName = "LinearPixels"
  case class LinearPixelsConfig(trainLocation: String = "", testLocation: String = "")

  def run(sc: SparkContext, config: LinearPixelsConfig) = {
    val numClasses = 10

    // Load and cache the training data.
    val trainData = CifarLoader(sc, config.trainLocation).cache

    // A featurizer maps input images into vectors. For this pipeline, we'll also convert the image to grayscale.
    val featurizer = ImageExtractor then GrayScaler then ImageVectorizer
    val labelExtractor = LabelExtractor then ClassLabelIndicatorsFromIntLabels(numClasses) then new Cacher[DenseVector[Double]]

    // Our training features are the featurizer applied to our training data.
    val trainFeatures = featurizer(trainData)
    val trainLabels = labelExtractor(trainData)

    // We estimate our model as by calling a linear solver on our data.
    val model = LinearMapEstimator().fit(trainFeatures, trainLabels)

    // The final prediction pipeline is the composition of our featurizer and our model.
    // Since we end up using the results of the prediction twice, we'll add a caching node.
    val predictionPipeline = featurizer then model then new Cacher[DenseVector[Double]]

    // Calculate training error.
    val trainError = Stats.classificationError(predictionPipeline(trainData), trainLabels)

    // Do testing.
    val testData = CifarLoader(sc, config.testLocation)
    val testLabels = labelExtractor(testData)

    val testError = Stats.classificationError(predictionPipeline(testData), testLabels)

    logInfo(s"Training error is: $trainError, Test error is: $testError")

    predictionPipeline
  }

  def parse(args: Array[String]): LinearPixelsConfig = new OptionParser[LinearPixelsConfig](appName) {
    head(appName, "0.1")
    opt[String]("trainLocation") required() action { (x,c) => c.copy(trainLocation=x) }
    opt[String]("testLocation") required() action { (x,c) => c.copy(testLocation=x) }
  }.parse(args, LinearPixelsConfig()).get

  /**
   * The actual driver receives its configuration parameters from spark-submit usually.
   * @param args
   */
  def main(args: Array[String]) = {
    val conf = new SparkConf().setAppName(appName)
    conf.setIfMissing("spark.master", "local[2]") // This is a fallback if things aren't set via spark submit.

    val sc = new SparkContext(conf)

    val appConfig = parse(args)
    run(sc, appConfig)

    sc.stop()
  }

}