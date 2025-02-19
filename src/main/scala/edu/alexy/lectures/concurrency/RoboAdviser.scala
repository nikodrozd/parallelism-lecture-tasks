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
    case Success(Some(value)) => Success(value.revenue)
    case _ => Failure(DbError)
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
  def getAllTickersPrices: Future[Seq[(String, Double)]] = getAllTickersRetryable().flatMap(tickerSeq => Future.sequence(tickerSeq.map(ticker => getPriceRetryable(ticker).map((ticker, _)))))

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
  } yield result.filter{ case (company, price) => company.isCheap(price) }

  // Task 8.
  // Implement a function that returns a list of expensive ('Company.isExpensive') companies
  // with their real time stock prices using 'getAllTickersRetryable', 'getCompanyRetryable',
  // 'getPriceRetryable' and zipping.
  def sellList: Future[Seq[(Company, Double)]] = {
    def getEntitiesByFuncRetriable[A] (f: (String, Int) => Future[A], retries: Int = 10): Future[Seq[A]] =
      getAllTickersRetryable().flatMap(tickerSeq => Future.sequence(tickerSeq.map(f(_, retries))))

    val companies: Future[Seq[Option[Company]]] = getEntitiesByFuncRetriable(getCompanyRetryable)
    val prices: Future[Seq[Double]] = getEntitiesByFuncRetriable(getPriceRetryable)
    val resultNotFiltered = companies.zip(prices).map{case (companySeq, priceSeq) => companySeq zip priceSeq}
    resultNotFiltered.map(_ collect {case (Some(company), price) if company.isExpensive(price) => (company, price)})
  }

}
