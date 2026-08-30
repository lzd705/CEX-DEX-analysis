// SPDX-License-Identifier: MIT
pragma solidity 0.8.36;

contract TwoVenueV2Executor {
    enum Direction {
        UniswapToSushi,
        SushiToUniswap
    }

    error Unauthorized();
    error ZeroInput();
    error ReviewedIdentityMismatch();
    error ExternalCallFailed();
    error UnexpectedResidualState();

    address private constant UNI = 0x1f9840a85d5aF5bf1D1762F925BDADdC4201F984;
    address private constant WETH = 0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2;
    address private constant UNISWAP_ROUTER = 0x7a250d5630B4cF539739dF2C5dAcb4c659F2488D;
    address private constant SUSHISWAP_ROUTER = 0xd9e1cE17f2641f24aE83637ab66a2cca9C378B9F;
    address private constant UNISWAP_FACTORY = 0x5C69bEe701ef814a2B6a3EDD4B1652CB9cc5aA6f;
    address private constant SUSHISWAP_FACTORY = 0xC0AEe478e3658e2610c5F7A4A2E1777cE9e4f2Ac;
    address private constant AUTHORIZED_SENDER = 0x5CA9E6c3Ed27Cc0AcFb355061FcaB6964D4Fc444;

    bytes4 private constant BALANCE_OF = 0x70a08231;
    bytes4 private constant APPROVE = 0x095ea7b3;
    bytes4 private constant FACTORY = 0xc45a0155;
    bytes4 private constant ROUTER_WETH = 0xad5c4648;
    bytes4 private constant SWAP_EXACT_TOKENS_FOR_TOKENS = 0x38ed1739;

    constructor() {
        if (
            _readAddress(UNISWAP_ROUTER, FACTORY) != UNISWAP_FACTORY
                || _readAddress(UNISWAP_ROUTER, ROUTER_WETH) != WETH
                || _readAddress(SUSHISWAP_ROUTER, FACTORY) != SUSHISWAP_FACTORY
                || _readAddress(SUSHISWAP_ROUTER, ROUTER_WETH) != WETH
        ) {
            revert ReviewedIdentityMismatch();
        }
        _approve(UNI, UNISWAP_ROUTER);
        _approve(UNI, SUSHISWAP_ROUTER);
        _approve(WETH, UNISWAP_ROUTER);
        _approve(WETH, SUSHISWAP_ROUTER);
    }

    function execute(Direction direction, uint256 amountWethIn)
        external
        returns (uint256 intermediateUni, uint256 finalWeth)
    {
        if (msg.sender != AUTHORIZED_SENDER) revert Unauthorized();
        if (amountWethIn == 0) revert ZeroInput();
        if (
            address(this).balance != 0 || _balanceOf(UNI) != 0
                || _balanceOf(WETH) != amountWethIn
        ) {
            revert UnexpectedResidualState();
        }

        address firstRouter;
        address secondRouter;
        if (direction == Direction.UniswapToSushi) {
            firstRouter = UNISWAP_ROUTER;
            secondRouter = SUSHISWAP_ROUTER;
        } else {
            firstRouter = SUSHISWAP_ROUTER;
            secondRouter = UNISWAP_ROUTER;
        }

        address[] memory firstPath = new address[](2);
        firstPath[0] = WETH;
        firstPath[1] = UNI;
        _swap(firstRouter, amountWethIn, firstPath);
        if (_balanceOf(WETH) != 0) revert UnexpectedResidualState();
        intermediateUni = _balanceOf(UNI);
        if (intermediateUni == 0) revert UnexpectedResidualState();

        address[] memory secondPath = new address[](2);
        secondPath[0] = UNI;
        secondPath[1] = WETH;
        _swap(secondRouter, intermediateUni, secondPath);
        if (address(this).balance != 0 || _balanceOf(UNI) != 0) {
            revert UnexpectedResidualState();
        }
        finalWeth = _balanceOf(WETH);
    }

    function _swap(address router, uint256 amountIn, address[] memory path) private {
        (bool success,) = router.call(
            abi.encodeWithSelector(
                SWAP_EXACT_TOKENS_FOR_TOKENS,
                amountIn,
                uint256(0),
                path,
                address(this),
                block.timestamp + 60
            )
        );
        if (!success) revert ExternalCallFailed();
    }

    function _approve(address token, address spender) private {
        (bool success, bytes memory result) = token.call(
            abi.encodeWithSelector(APPROVE, spender, type(uint256).max)
        );
        if (!success || (result.length != 0 && !abi.decode(result, (bool)))) {
            revert ExternalCallFailed();
        }
    }

    function _balanceOf(address token) private view returns (uint256 value) {
        (bool success, bytes memory result) = token.staticcall(
            abi.encodeWithSelector(BALANCE_OF, address(this))
        );
        if (!success || result.length != 32) revert ExternalCallFailed();
        value = abi.decode(result, (uint256));
    }

    function _readAddress(address target, bytes4 selector) private view returns (address value) {
        (bool success, bytes memory result) = target.staticcall(
            abi.encodeWithSelector(selector)
        );
        if (!success || result.length != 32) revert ReviewedIdentityMismatch();
        value = abi.decode(result, (address));
    }
}
