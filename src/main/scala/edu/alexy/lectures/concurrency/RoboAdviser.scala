package edu.alexy.lectures.concurrency

import edu.alexy.lectures.concurrency.model.Company
import edu.alexy.lectures.concurrency.util.{Db, DbError, WebApi, WebApiError}
import scala.concurrent.Future

import scala.util.{Failure, Success}
import scala.concurrent.ExecutionContext.Implicits.global

object RoboAdviser {
  // Task 1.
  // Return 'AAPL' revenue from `Db.getCompanyLastFinancials`. Possible error should be returned as a ServiceError.
  def getAAPLRevenue: Future[Double] = Db.getCompanyLastFinancials("AAPL").transform {
    case Success(value) => Success(value.map(_.revenue.toDouble).getOrElse(0.0))
    case Failure(_) => Failure(DbError)
  }

  // Task 2.
  // Implement a fallback strategy for 'Db.getAllTickers'.
  // 'Db.getAllTickers' should be called no more then 'retries' times.
  // Possible error should be returned as a ServiceError.
  def getAllTickersRetryable(retries: Int = 10): Future[Seq[String]] = {
    val res = Db.getAllTickers
    if (retries <= 0) res.transform {
      case Success(value) => Success(value)
      case Failure(_) => Failure(DbError)
    } else res.fallbackTo(getAllTickersRetryable(retries - 1))
  }

  // Task 3.
  // Implement a fallback strategy for 'Db.getCompanyLastFinancials'.
  // 'Db.getCompanyLastFinancials' should be called no more then 'retries' times.
  // Possible error should be returned as a ServiceError.
  def getCompanyRetryable(ticker: String, retries: Int = 10): Future[Option[Company]] = {
    val res = Db.getCompanyLastFinancials(ticker)
    if (retries <= 0) res.transform {
      case Success(value) => Success(value)
      case Failure(_) => Failure(DbError)
    } else res.fallbackTo(getCompanyRetryable(ticker, retries - 1))
  }

  // Task 4.
  // Implement a fallback strategy 'WebApi.getPrice'.
  // 'WebApi.getPrice' should be called no more then 'retries' times.
  // Possible error should be returned as a ServiceError.
  def getPriceRetryable(ticker: String, retries: Int = 10): Future[Double] = {
    val res: Future[Double] = WebApi.getPrice(ticker)
    if (retries <= 0) res.transform {
      case Success(value) => Success(value)
      case Failure(_) => Failure(WebApiError)
    } else res.fallbackTo(getPriceRetryable(ticker, retries - 1))
  }

  // Task 5.
  // Using retryable functions return all tickers with their real time prices.
  def getAllTickersPrices: Future[Seq[(String, Double)]] = {
    val tickers: Future[Seq[String]] = getAllTickersRetryable()
    val prices: Future[Seq[Double]] = tickers.flatMap(x => Future.sequence(x.map(getPriceRetryable(_))))
    for {
      ticker <- tickers
      price <- prices
    } yield ticker zip price
  }

  // Task 6.
  // Using `getCompanyRetryable` and `getPriceRetryable` functions return a company with its real time stock price.
  def getCompanyFinancialsWithPrice(ticker: String): Future[(Company, Double)] = getCompanyRetryable(ticker).transform {
      case Success(Some(value)) => Success(value)
      case Success(None) => Failure(DbError)
      case Failure(exception) => Failure(exception)
    } zip getPriceRetryable(ticker)

  // Task 7.
  // Implement a function that returns a list of chip ('Company.isCheap') companies
  // with their real time stock prices using 'getAllTickersRetryable' and
  // 'getCompanyFinancialsWithPrice' functions.
  def buyList: Future[Seq[(Company, Double)]] = for {
    tickerSeq <- getAllTickersRetryable()
    result <- Future.sequence(tickerSeq.map(getCompanyFinancialsWithPrice))
  } yield result.filter(x => x._1.isCheap(x._2))

  // Task 8.
  // Implement a function that returns a list of expensive ('Company.isExpensive') companies
  // with their real time stock prices using 'getAllTickersRetryable', 'getCompanyRetryable',
  // 'getPriceRetryable' and zipping.
  def sellList: Future[Seq[(Company, Double)]] = {
    val tickers: Future[Seq[String]] = getAllTickersRetryable()
    val companies: Future[Seq[Option[Company]]] = tickers.flatMap(x => Future.sequence(x.map(getCompanyRetryable(_))))
    val prices: Future[Seq[Double]] = tickers.flatMap(x => Future.sequence(x.map(getPriceRetryable(_))))
    for {
      price: Seq[Double] <- prices
      company: Seq[Option[Company]] <- companies
    } yield (company.flatten zip price).filter(x => x._1.isExpensive(x._2))
  }

}
