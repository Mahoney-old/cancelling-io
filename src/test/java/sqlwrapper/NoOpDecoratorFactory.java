package sqlwrapper;

public class NoOpDecoratorFactory<T, F> implements DecoratorFactory<T, F> {
    @Override
    public T decorate(T toDecorate, F factory) {
        return toDecorate;
    }
}
