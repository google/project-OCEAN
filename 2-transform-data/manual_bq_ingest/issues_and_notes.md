# issues and notes

## garbled?/weird message headers…

Noticed in `gs://mailman-python-ideas-gzip/2020-03.mbox.gz`.  In some cases I see headers like this, with the same unusual email address associated with multiple people.  This doesn’t seem to happen very often.
e.g.:
	From mail.python.org@marco.sulla.e4ward.com Thu Mar 12 19:05:24 2020
	From: Marco Sulla <mail.python.org@marco.sulla.e4ward.com>
… but also…
	From mail.python.org@marco.sulla.e4ward.com Thu Mar 12 16:47:03 2020
	From: Christopher Barker <mail.python.org@marco.sulla.e4ward.com>
… and others.

