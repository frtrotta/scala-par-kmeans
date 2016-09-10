package kmeans

import java.util.concurrent._
import scala.collection._
import org.scalatest.FunSuite
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import common._
import scala.math._
import org.scalacheck.Arbitrary
import org.scalatest.prop.Checkers
import org.scalacheck.Prop._

object KM extends KMeans
import KM._

@RunWith(classOf[JUnitRunner])
class KMeansSuite extends FunSuite with Checkers{

  def checkClassify(points: GenSeq[Point], means: GenSeq[Point], expected: GenMap[Point, GenSeq[Point]]) {
    assert(KM.classify(points, means) == expected,
      s"classify($points, $means) should equal to $expected")
  }

  test("'classify should work for empty 'points' and empty 'means'") {
    val points: GenSeq[Point] = IndexedSeq()
    val means: GenSeq[Point] = IndexedSeq()
    val expected = GenMap[Point, GenSeq[Point]]()
    checkClassify(points, means, expected)
  }

  test("'classify' should work for empty 'points' and 'means' == GenSeq(Point(1,1,1))") {
    val points: GenSeq[Point] = IndexedSeq()
    val mean = new Point(1, 1, 1)
    val means: GenSeq[Point] = IndexedSeq(mean)
    val expected = GenMap[Point, GenSeq[Point]]((mean, GenSeq()))
    checkClassify(points, means, expected)
  }

  test("'classify' should work for 'points' == GenSeq((1, 1, 0), (1, -1, 0), (-1, 1, 0), (-1, -1, 0)) and 'means' == GenSeq((0, 0, 0))") {
    val p1 = new Point(1, 1, 0)
    val p2 = new Point(1, -1, 0)
    val p3 = new Point(-1, 1, 0)
    val p4 = new Point(-1, -1, 0)
    val points: GenSeq[Point] = IndexedSeq(p1, p2, p3, p4)
    val mean = new Point(0, 0, 0)
    val means: GenSeq[Point] = IndexedSeq(mean)
    val expected = GenMap((mean, GenSeq(p1, p2, p3, p4)))
    checkClassify(points, means, expected)
  }

  test("'classify' should work for 'points' == GenSeq((1, 1, 0), (1, -1, 0), (-1, 1, 0), (-1, -1, 0)) and 'means' == GenSeq((1, 0, 0), (-1, 0, 0))") {
    val p1 = new Point(1, 1, 0)
    val p2 = new Point(1, -1, 0)
    val p3 = new Point(-1, 1, 0)
    val p4 = new Point(-1, -1, 0)
    val points: GenSeq[Point] = IndexedSeq(p1, p2, p3, p4)
    val mean1 = new Point(1, 0, 0)
    val mean2 = new Point(-1, 0, 0)
    val means: GenSeq[Point] = IndexedSeq(mean1, mean2)
    val expected = GenMap((mean1, GenSeq(p1, p2)), (mean2, GenSeq(p3, p4)))
    checkClassify(points, means, expected)
  }

  def checkParClassify(points: GenSeq[Point], means: GenSeq[Point], expected: GenMap[Point, GenSeq[Point]]) {
    assert(KM.classify(points.par, means.par) == expected,
      s"classify($points par, $means par) should equal to $expected")
  }

  test("'classify with data parallelism should work for empty 'points' and empty 'means'") {
    val points: GenSeq[Point] = IndexedSeq()
    val means: GenSeq[Point] = IndexedSeq()
    val expected = GenMap[Point,GenSeq[Point]]()
    checkParClassify(points, means, expected)
  }

//  val pointGen: org.scalacheck.Gen[Point] = for {
//    x <- Arbitrary.arbitrary[Double]
//    y <- Arbitrary.arbitrary[Double]
//    z <- Arbitrary.arbitrary[Double]
//  } yield new Point(x, y, z)

  implicit def arbitraryPoint: Arbitrary[Point] = Arbitrary {
    for {
      x <- Arbitrary.arbitrary[Double]
      y <- Arbitrary.arbitrary[Double]
      z <- Arbitrary.arbitrary[Double]
    } yield new Point(x, y, z)
  }

//  val pointSeqGen = org.scalacheck.Gen.containerOf[Seq, Point](pointGen)
//
//  def meansGen(n: Int) = org.scalacheck.Gen.containerOf[Seq, Point](pointGen) suchThat(_.size <= n)

  test("classify properties") {
    check(forAll { (points: List[Point], means: List[Point]) =>
      (!points.isEmpty && !means.isEmpty) ==> (KM.classify(points, means).size == means.size)
    })
  }

  test("update properties") {
    check(forAll { (points: List[Point], means: List[Point]) =>
      (!points.isEmpty && !means.isEmpty) ==> (KM.update(KM.classify(points, means), means).size == means.size)
    })
  }
}


  
