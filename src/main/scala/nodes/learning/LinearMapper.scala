package nodes.learning

import breeze.linalg._
import edu.berkeley.cs.amplab.mlmatrix.{NormalEquations, RowPartitionedMatrix}
import org.apache.spark.rdd.RDD
import pipelines.Pipelines._
import utils.MatrixUtils

import scala.reflect.ClassTag

case class LinearMapper(x: DenseMatrix[Double])
  extends Transformer[DenseVector[Double], DenseVector[Double]]
  with Serializable {

  /**
   * Apply a linear model to an input.
   * @param in Input.
   * @return Output.
   */
  def transform(in: DenseVector[Double]): DenseVector[Double] = {
    x.t * in
  }

  /**
   * Apply a linear model to a collection of inputs.
   *
   * @param in Collection of A's.
   * @return Collection of B's.
   */
  def apply(in: RDD[DenseVector[Double]]): RDD[DenseVector[Double]] = {
    val modelBroadcast = in.context.broadcast(x)
    in.mapPartitions(rows => {
      val mat = MatrixUtils.arrayToMatrix(rows) * modelBroadcast.value
      MatrixUtils.matrixToRowArray(mat).iterator
    })
  }
}
/**
 * Linear Map Estimator. Solves an OLS problem on data given labels and emits a LinearMapper transformer.
 *
 * @param lambda L2 Regularization parameter
 */
class LinearMapEstimator(lambda: Option[Double] = None)
    extends LabelEstimator[RDD[DenseVector[Double]], RDD[DenseVector[Double]], RDD[DenseVector[Double]]]
    with Serializable {

  /**
   * Learns a linear model (OLS) based on training features and training labels.
   * If the regularization parameter is set
   *
   * @param trainingFeatures Training features.
   * @param trainingLabels Training labels.
   * @return
   */
  def fit(
      trainingFeatures: RDD[DenseVector[Double]],
      trainingLabels: RDD[DenseVector[Double]]): LinearMapper = {

    val A = RowPartitionedMatrix.fromArray(trainingFeatures.map(x => x.toArray))
    val b = RowPartitionedMatrix.fromArray(trainingLabels.map(x => x.toArray))

    val x = lambda match {
      case Some(l) => new NormalEquations().solveLeastSquaresWithL2(A, b, l)
      case None => new NormalEquations().solveLeastSquares(A, b)
    }

    LinearMapper(x)
  }
}

/**
 * Companion object to LinearMapEstimator that allows for construction without new.
 */
object LinearMapEstimator extends Serializable {
  def apply(lambda: Option[Double] = None) = new LinearMapEstimator(lambda)
}