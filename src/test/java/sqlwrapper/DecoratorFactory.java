package sqlwrapper;

public interface DecoratorFactory<T, F> {
    public T decorate(T toDecorate, F connectionWrapper);
}
