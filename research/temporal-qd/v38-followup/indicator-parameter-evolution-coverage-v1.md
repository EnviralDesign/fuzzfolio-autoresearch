# Indicator-parameter evolution coverage v1

Source-grounded audit of the current `evolvable_resource_v1` parameter surface. This is not a launch.

- Catalog indicators: **88**
- With period surface: **79** (0.8977272727272727)
- With any parameter surface: **86** (0.9772727272727273)
- V38 bound parent instances: **17**
- Bound instances whose catalog row has a period surface: **15**

Period mutation admits only `talibMeta` parameters whose name contains `period` and whose `uiType` is `integer_slider` or `float_slider`. Choices are exactly fast / nominal / slow.

Do not add generic numeric mutation. Any missing surface must be catalog-bound, identity-hashed, and tested.

Report sha: `sha256:58b753caac456e6baf5593e493a0bd9cdbb045c517ee7c093867ba7980aa0e9c`
