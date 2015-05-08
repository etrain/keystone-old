package nodes

import utils.{RowColumnMajorByteArrayVectorizedImage, ImageMetadata, LabeledImage}
import pipelines.Transformer
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream
import java.util.zip.GZIPInputStream
import java.io.{BufferedInputStream, FileInputStream}


/**
 * Loads images from the CIFAR-10 Dataset.
 */
object CifarLoader {
  //We hardcode this because these are properties of the CIFAR-10 dataset.
  val nrow = 32
  val ncol = 32
  val nchan = 3

  val labelSize = 1

  def cifar10ToBufferedImage(cifar: Array[Byte]): RowColumnMajorByteArrayVectorizedImage = {
    val rowlen = ncol*nchan
    val byteLen = nrow*rowlen

    //Allocate some space for the rows.
    assert(cifar.length == byteLen, "CIFAR-10 Images MUST be 32x32x3.")

    RowColumnMajorByteArrayVectorizedImage(cifar, ImageMetadata(nrow, ncol, nchan))
  }

  def loadLabeledImages(path: String): Seq[LabeledImage] = {
    val imgCount = labelSize + nrow*ncol*nchan

    val imageBytes = Array.fill[Byte](imgCount)(0x00)
    var out = Array[LabeledImage]()

    val inFile = new FileInputStream(path)

    println("About to read image")
    while(inFile.read(imageBytes, 0, imgCount) > 0) {
      val img = cifar10ToBufferedImage(imageBytes.tail)
      val label = imageBytes.head.toShort
      val li = LabeledImage(img, label)
      out = out :+ li
    }
    out
  }

  def apply(sc: SparkContext, path: String): RDD[LabeledImage] = {
    val images = CifarLoader.loadLabeledImages(path)

    sc.parallelize(images)
  }
}