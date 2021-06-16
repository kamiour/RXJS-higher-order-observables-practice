import { run } from './../03-utils';
import {
  catchError,
  mergeMap,
  concatMap,
  delay,
  exhaustMap,
  switchMap,
  take,
  filter,
  concatMapTo,
  mergeMapTo,
  switchMapTo,
  debounceTime,
  distinctUntilChanged,
  map,
  pluck,
  tap,
  scan,
  mapTo,
} from 'rxjs/operators';
import {
  fromEvent,
  range,
  interval,
  of,
  from,
  Observable,
  NEVER,
  concat,
  combineLatest,
  EMPTY,
} from 'rxjs';
import { fromFetch } from 'rxjs/fetch';

// Task 1. concatMap()
// Реализуйте функцию, которая создает Observable, который выдает числа в диапазоне от 1 до 10
// через случайное количество времени в диапазоне от 1с до 5с
// Используйте функцию randomDelay(), of(), concatMap(), delay()
// Проведите эксперимент заменяя метод concatMap на mergeMap, switchMap, exhaustMap
(function task1(): void {
  function randomDelay(min: number, max: number) {
    const pause = Math.floor(Math.random() * (max - min)) + min;
    console.log(pause);
    return pause;
  }

  const stream$ = range(1, 10).pipe(
    concatMap((number) => of(number).pipe(delay(randomDelay(1000, 5000))))
  );

  //   run(stream$);
})();

// Task 2. mergeMap()
// Испольуя функцию emulateHttpCall и массив идентификаторов ids
// организуйте получение объектов в параллель.
(function task2(): void {
  function emulateHttpCall(id: number): Observable<any> {
    switch (id) {
      case 1:
        return of({ id: 1, name: 'Anna' }).pipe(delay(4000)); // <-- emulation of http call, which returns Observable after 4s
      case 2:
        return of({ id: 2, name: 'Boris' }).pipe(delay(3000)); // <-- pause 3s
      case 3:
        return of({ id: 3, name: 'Clara' }).pipe(delay(2000)); // <-- pause 2s
    }
  }

  const ids = [1, 3, 2, 2, 3, 3, 1, 2, 3];

  const stream$ = of(...ids).pipe(
    mergeMap((number) => emulateHttpCall(number))
  );

  //   run(stream$);
})();

// Task 3.1. switchMap()
// Создайте внешний поток, используя fromFetch('https://api.github.com/users?per_page=5')
// Создайте для результата внешнего потока внутренний поток response.json(), используя switchMap()
// Дополнительно фильтруйте элементы внешнего потока по условию response.ok === true
(function task3_1(): void {
  const stream$ = fromFetch('https://api.github.com/users?per_page=5').pipe(
    filter((response) => response.ok),
    switchMap((response) => response.json())
  );
  //   run(stream$);
})();

// Task 3.2. switchMap() Author: Dzmitry Skurat
// Создайте поток по вводу количества записей на странице в текстовое поле.
// для запроса ajax(`https://api.github.com/users?per_page=${perPage}`)
// Чтобы не спамить сервер поставте задердку в 1000мс через debounceTime().
// С помощью distinctUntilChanged() исключите отправку двух повторных запросов на сервер.
// Выведите массив логинов.
// Операторы, которые могут понадобиться: switchMap(), debounceTime(), pluck(), map().
(function task3_2(): void {
  function getNumberOfRecords(): string {
    const numberOfRecords = (document.getElementById(
      'text-field'
    ) as HTMLInputElement).value;

    return numberOfRecords;
  }

  const runBtn = document.getElementById('runBtn');

  const clicks$ = fromEvent(runBtn, 'click').pipe(
    map(() => {
      return getNumberOfRecords();
    })
  );

  const stream$ = clicks$.pipe(
    distinctUntilChanged(),
    debounceTime(1000),
    switchMap((numberOfRecords) => {
      return fromFetch(
        `https://api.github.com/users?per_page=${numberOfRecords}`
      ).pipe(
        switchMap((response) => response.json()),
        map((users) => users.map((user) => user.login))
      );
    })
  );

  //   run(stream$);

  // I did not figure out how to use pluck here
})();

// Task 4. exhaustMap()
// Создайте внешний поток из событий click по кнопке runBtn.
// Во время первого клика по кнопке создайте внутренний поток, используя interval(1000)
// Элементы внутреннего потока должны попасть в выходной поток.
// Игнорируйте все последующие клики на кнопке
(function task4() {
  const runBtn = document.getElementById('runBtn');
  const clicks$ = fromEvent(runBtn, 'click');
  const stream$ = clicks$.pipe(
    exhaustMap((event) => interval(1000).pipe(take(5)))
  );
  //   run(stream$);
})();

// Task 5. concatMapTo()
// Создайте внешний поток событий click по кнопке runBtn.
// Во время клика по кнопке, создайте внутренний поток из слов
// 'Hello', 'World!', используя of() и объедините его с потоком NEVER
// Добавьте слова внутреннего потока в результирующий поток
// Обясните результат нескольких кликов по кнопке
(function task5() {
  const runBtn = document.getElementById('runBtn');

  const outerObservable = fromEvent(runBtn, 'click');
  const innerObservable = concat(of('Hello', 'World!'), NEVER);

  const stream$ = outerObservable.pipe(concatMapTo(innerObservable));
  //   run(stream$);

  // Все клики после первого не работают, так как первый observable не комплитится из-за NEVER
})();

// Task 6. mergeMapTo()
// Задание аналогично предыдущему, только теперь вместо concatMap используйте mergeMap
// Обясните результат нескольких кликов по кнопке
(function task6() {
  const runBtn = document.getElementById('runBtn');

  const outerObservable = fromEvent(runBtn, 'click');
  const innerObservable = concat(of('Hello', 'World!'), NEVER);

  const stream$ = outerObservable.pipe(mergeMapTo(innerObservable));
  //   run(stream$);

  // Все клики после первого работают, так как обрабатываются параллельно (следующий обработчик не ждет завершения предыдущего)
})();

// Task7. switchMapTo()
// Создайте внешний поток событий click по кнопке runBtn.
// Во время клика по кнопке, создайте внутренний поток,
// который будет выдавать числа от 0 до 4 с интервалом в 1с.
// Каждый новый клик по кнопке должен начинать выдавать значения внутреннего потока
// начииная с 0, недожидаясь завершения выдачи всех предыдущих чисел.
(function task7() {
  const runBtn = document.getElementById('runBtn');

  const outerObservable = fromEvent(runBtn, 'click');
  const innerObservable = interval(1000).pipe(take(5));

  const stream$ = outerObservable.pipe(switchMapTo(innerObservable));
  //   run(stream$);
})();

// My tasks
// switchMap()
// Сделайте запрос на получение GitHub юзера по логину из инпута.
// Если такого юзера нет, сделать запрос на какого - то дефолтного юзера(например, 'mojombo')
(function myTask1() {
  function getGithubUsername(): string {
    const username = (document.getElementById('text-field') as HTMLInputElement)
      .value;

    return username;
  }

  const defaultUser = 'mojombo';

  const runBtn = document.getElementById('runBtn');

  const clicks$ = fromEvent(runBtn, 'click').pipe(
    map(() => getGithubUsername())
  );

  const defaultUserObs$ = fromFetch(
    `https://api.github.com/users/${defaultUser}`
  ).pipe(switchMap((response) => response.json()));

  const stream$ = clicks$.pipe(
    switchMap((username) => {
      return fromFetch(`https://api.github.com/users/${username}`).pipe(
        switchMap(
          (response) => (response.ok ? response.json() : defaultUserObs$)
        )
      );
    })
  );

  //   run(stream$);
})();

// exhaustMap()
// Сделайте запрос на получение юзера по id (значение из инпута) (используя emulateHttpCall() функцию)
// так, чтобы следующий клик не обрабатывался, пока не обработается предыдущий
(function myTask2() {
  function getUserId(): number {
    const userId = (document.getElementById('text-field') as HTMLInputElement)
      .value;

    return +userId;
  }

  function emulateHttpCall(id: number): Observable<any> {
    function randomDelay(min: number, max: number) {
      const pause = Math.floor(Math.random() * (max - min)) + min;
      return pause;
    }

    return of({ id, name: `user-${id}` }).pipe(delay(randomDelay(3000, 5000)));
  }

  const runBtn = document.getElementById('runBtn');

  const clicks$ = fromEvent(runBtn, 'click').pipe(map(() => getUserId()));

  const stream$ = clicks$.pipe(exhaustMap((id) => emulateHttpCall(id)));

  //   run(stream$);
})();

export function runner() {}
