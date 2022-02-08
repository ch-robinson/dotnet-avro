namespace Chr.Avro.Abstract
{
    using System;

    public class ObjectDefaultValue<TUnderlying> : DefaultValue
    {
        private readonly TUnderlying value;

        public ObjectDefaultValue(Schema schema, TUnderlying value)
            : base(schema)
        {
            this.value = value;
        }

        public override T ToObject<T>()
        {
            object boxed = value;

            if (boxed == null)
            {
                return default;
            }
            else if (typeof(T).IsAssignableFrom(boxed.GetType()))
            {
                return (T)boxed;
            }
            else
            {
                throw new NotImplementedException();
            }
        }
    }
}
