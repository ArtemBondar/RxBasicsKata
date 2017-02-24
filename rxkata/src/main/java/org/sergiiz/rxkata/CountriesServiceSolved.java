package org.sergiiz.rxkata;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.FutureTask;

import io.reactivex.Observable;
import io.reactivex.ObservableSource;
import io.reactivex.Single;
import io.reactivex.SingleSource;
import io.reactivex.functions.BiFunction;
import io.reactivex.functions.Function;
import io.reactivex.functions.Predicate;

class CountriesServiceSolved implements CountriesService {


    @Override
    public Single<String> countryNameInCapitals(Country country) {
        return Single.just(country.getName()).map(String::toUpperCase);
    }

    public Single<Integer> countCountries(List<Country> countries) {
        return Single.fromObservable(Observable.just(countries)).flatMap(new Function<List<Country>, SingleSource<Integer>>() {
            @Override
            public SingleSource<Integer> apply(List<Country> countries) throws Exception {
                return Single.just(countries.size());
            }
        });
    }

    public Observable<Long> listPopulationOfEachCountry(List<Country> countries) {
        return Observable.fromIterable(countries).flatMap(new Function<Country, ObservableSource<Long>>() {
            @Override
            public ObservableSource<Long> apply(Country country) throws Exception {
                return Observable.just(country.getPopulation());
            }
        });
    }

    @Override
    public Observable<String> listNameOfEachCountry(List<Country> countries) {
        return Observable.fromIterable(countries).flatMap(new Function<Country, ObservableSource<String>>() {
            @Override
            public ObservableSource<String> apply(Country country) throws Exception {
                return Observable.just(country.getName());
            }
        });
    }

    @Override
    public Observable<Country> listOnly3rdAnd4thCountry(List<Country> countries) {
        return Observable.fromIterable(countries).skip(2).take(2);
    }

    @Override
    public Single<Boolean> isAllCountriesPopulationMoreThanOneMillion(List<Country> countries) {
        return Observable.fromIterable(countries).all(country -> country.getPopulation() > 1000000);
    }

    @Override
    public Observable<Country> listPopulationMoreThanOneMillion(List<Country> countries) {
        return Observable.fromIterable(countries).filter(country -> country.getPopulation() > 1000000);
    }

    // TODO: 23.02.2017
    // FIXME: 23.02.2017 fail by timeout
    @Override
    public Observable<Country> listPopulationMoreThanOneMillionWithTimeoutFallbackToEmpty(final FutureTask<List<Country>> countriesFromNetwork) {
        return Observable.fromFuture(countriesFromNetwork).flatMap(new Function<List<Country>, ObservableSource<Country>>() {
            @Override
            public ObservableSource<Country> apply(List<Country> countries) throws Exception {
                return Observable.fromIterable(countries);
            }
        }).filter(new Predicate<Country>() {
            @Override
            public boolean test(Country country) throws Exception {
                if (country.getPopulation() > 1000000) {
                    return true;
                }
                return false;
            }
        });
    }

    @Override
    public Observable<String> getCurrencyUsdIfNotFound(String countryName, List<Country> countries) {
        return Observable.fromIterable(countries).filter(new Predicate<Country>() {
            @Override
            public boolean test(Country country) throws Exception {
                if (country.getName().equals(countryName)) {
                    return true;
                } else {
                    return false;
                }
            }
        }).flatMap(new Function<Country, ObservableSource<String>>() {
            @Override
            public ObservableSource<String> apply(Country country) throws Exception {
                return Observable.just(country.getCurrency());
            }
        }).defaultIfEmpty("USD");
    }

    @Override
    public Observable<Long> sumPopulationOfCountries(List<Country> countries) {
        return Observable.fromIterable(countries).flatMap(new Function<Country, ObservableSource<Long>>() {
            @Override
            public ObservableSource<Long> apply(Country country) throws Exception {
                return Observable.just(country.getPopulation());
            }
        }).reduce((aLong, aLong2) -> aLong += aLong2).toObservable();
    }

    @Override
    public Single<Map<String, Long>> mapCountriesToNamePopulation(List<Country> countries) {
        return Observable.fromIterable(countries).toMap(country -> country.getName()).map(stringCountryMap -> {
            Map<String, Long> map = new HashMap<>();
            for (Country country : stringCountryMap.values()) {
                for (String s : stringCountryMap.keySet()) {
                    if (country.getName().equals(s)) {
                        map.put(s, country.getPopulation());
                        break;
                    }
                }
            }
            return map;
        });
    }

    @Override
    public Observable<Long> sumPopulationOfCountries(Observable<Country> countryObservable1,
                                                     Observable<Country> countryObservable2) {
        Observable<Long> first = countryObservable1.flatMap(new Function<Country, ObservableSource<Long>>() {
            @Override
            public ObservableSource<Long> apply(Country country) throws Exception {
                return Observable.just(country.getPopulation());
            }
        }).reduce(new BiFunction<Long, Long, Long>() {
            @Override
            public Long apply(Long aLong, Long aLong2) throws Exception {
                return aLong+=aLong2;
            }
        }).toObservable();
        Observable<Long> second = countryObservable2.flatMap(new Function<Country, ObservableSource<Long>>() {
            @Override
            public ObservableSource<Long> apply(Country country) throws Exception {
                return Observable.just(country.getPopulation());
            }
        }).reduce(new BiFunction<Long, Long, Long>() {
            @Override
            public Long apply(Long aLong, Long aLong2) throws Exception {
                return aLong+=aLong2;
            }
        }).toObservable();
        return Observable.combineLatest(first, second, new BiFunction<Long, Long, Long>() {
            @Override
            public Long apply(Long aLong, Long aLong2) throws Exception {
                return aLong+=aLong2;
            }
        });
    }

    @Override
    public Single<Boolean> areEmittingSameSequences(Observable<Country> countryObservable1,
                                                    Observable<Country> countryObservable2) {
        return Observable.sequenceEqual(countryObservable1, countryObservable2);
    }
}