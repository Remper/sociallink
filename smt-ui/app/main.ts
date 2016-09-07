import { platformBrowserDynamic } from '@angular/platform-browser-dynamic';
import { SMT }                    from './app.smt';

const platform = platformBrowserDynamic();
platform.bootstrapModule(SMT);
