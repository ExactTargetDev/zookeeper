using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace ZooKeeperNet
{
    using System.IO;

    internal class AuthFailedException : IOException
    {
        public AuthFailedException(string msg)
            : base(msg)
        {
        }
    }
}
