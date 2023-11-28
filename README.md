# BigData_Lab3
Читоркин Егор, 6133
Задачи решены на Scala.

### RideCleanisingExercise

Фильтр написан на основе метода isInNYC() из пакета GeoUtils. Так, если начальная и конечная точки принадлежат NYC, фильтр возвращает true. Иначе false.

```scala
val filteredRides = rides.filter(ride => GeoUtils.isInNYC(ride.startLon, ride.startLat) && GeoUtils.isInNYC(ride.endLon, ride.endLat))
```

Скрин прохождения теста\
![test](/img/cleansing_test.jpg)

### RidesAndFaresExercise

Были реализованы два метода: flatMap1 и flatMap2.

В методе flatMap1 происходит следующее: встречается событие TaxiRide; если в fareState есть соответствующий TaxiFare (т.е. ```fareState.value != null```), то мы сбрасываем fareState и создаем новый кортеж из TaxiRide и TaxiFare; иначе добавляем этот TaxiRide в rideState.

```scala
val fare = fareState.value
if (fare != null) {
  fareState.clear()
  out.collect((ride, fare))
}
else {
  rideState.update(ride)
}
```

В методе flatMap2 происходят аналогичные действия: встречается событие TaxiFare; если в rideState есть соответствующий TaxiRide (т.е. ```rideState.value != null```), то мы сбрасываем rideState исоздаем новый кортеж из TaxiRide и TaxiFare; иначе добавляем этот TaxiFare в fareState.

```scala
val ride = rideState.value
if (ride != null) {
  rideState.clear()
  out.collect((ride, fare))
}
else {
  fareState.update(fare)
}
```

Скрин прохождения теста\
![test](/img/rides_test.jpg)

### HourlyTipsExerxise

Маппим в пары (driverId, tip), группируем их по id водителя, далее делим на часы, после чего проводим редукцию, находя сумму. Далее происходит перегруппировка по часам, для каждого часа находится максимум.

```scala
val hourlyTips = fares.map((f: TaxiFare) => (f.driverId, f.tip))
                      .keyBy(f => f._1)
                      .timeWindow(Time.hours(1))
                      .reduce((f_1: (Long, Float), f_2: (Long, Float)) => (f_1._1, f_1._2 + f_2._2), new WrapWithWindowInfo())
val hourlyMax = hourlyTips.timeWindowAll(Time.hours(1)).maxBy(2)
```

Скрин прохождения теста\
![test](/img/hourly_test.jpg)

### ExpiringStateExercise

Метод processElement1: встречается TaxiRide; если есть соответствующий ему TaxiFare (т.е. ```fareState.value != null```), fareState сбрасывается, таймер сбрасывается, создается новый кортеж (TaxiRide, TaxiFare); иначе полученный TaxiRide добавляется в rideState, регистрируется таймер.

```scala
val fare = fareState.value
if (fare != null) {
  fareState.clear()
  context.timerService.deleteEventTimeTimer(ride.getEventTime)
  out.collect((ride, fare))
} else {
  rideState.update(ride)
  context.timerService.registerEventTimeTimer(ride.getEventTime)
}
```

Метод processElement2 по аналогии с предыдущим: встречается TaxiFare; если есть соответствующий ему TaxiRide (т.е. ```rideState.value != null```), rideState сбрасывается, таймер сбрасывается, создается новый кортеж (TaxiRide, TaxiFare); иначе полученный TaxiFare добавляется в fareState, регистрируется таймер.

```scala
val ride = rideState.value
if (ride != null) {
  rideState.clear()
  context.timerService.deleteEventTimeTimer(ride.getEventTime)
  out.collect((ride, fare))
} else {
  fareState.update(fare)
  context.timerService.registerEventTimeTimer(fare.getEventTime)
}
```

Метод onTimer: непосредственно таймер. При срабатывании таймера, который был зарегистрирован в одном из предыдущих методов, получается, что в течение времени, которое было указано при регистрации таймера, так и не поступило события, которое бы подошло и сбросило этот таймер. Тогда происходит проверка согласованности/несогласованности. Если вдруг есть ```fareState.value != null``` или ```rideState.value != null```, то мы выбрасываем их в unmatchedFares или unmatchedRides соответственно, по ле чего сбрасываем эти состояния.

```scala
if (fareState.value != null) {
  ctx.output(unmatchedFares, fareState.value)
  fareState.clear()
}
if (rideState.value != null) {
  ctx.output(unmatchedRides, rideState.value)
  rideState.clear()
}
```

Скрин прохождения теста\
![test](/img/expirinf_test.jpg)
